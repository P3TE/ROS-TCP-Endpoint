#  Copyright 2020 Unity Technologies
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import rospy
import time
# from std_msgs.msg import Time, Duration

# Constants
DEFAULT_TIME_SCALE=1.0
# The maximum number of values to store when calulating an approximate time scale
TIMING_HISTORY_COUNT=50
# If this many seconds pass without a /time_scale message, but /clock messages are 
# still being received then it will switch back to approximate time scale
NO_TIMESCALE_MESSAGE_TIMEOUT_SECONDS=1.0
# If this many seconds pass without a /clock message, 
# but then a new /clock message is received, it will clear the history
CLEAR_HISTORY_TIMEOUT=1.0
# If no /clock messages are received for this many seconds, it will pause time
NO_CLOCK_MESSAGE_AUTO_PAUSE_TIMEOUT_SECONDS=0.2

class ClockTiming():
    """
    A simple class used to store a tuple of <wall_time, clock_time>
    """

    def __init__(self, wall_time, clock_time: rospy.Time):
        self.wall_time = wall_time
        self.clock_time = clock_time
        


class ClockTimings():
    """
    A class used to store /clock message information for estimating timescale.
    """

    def __init__(self):

        self.use_time_scale_from_topic = False

        self.timings = []
        self.approximate_timescale = DEFAULT_TIME_SCALE

        self.time_of_last_received_time_scale = None
        self.time_scale_from_topic = DEFAULT_TIME_SCALE

        self.is_paused = False
        self.should_reset_clock_time = False

        self.any_clock_messages_received = False
        self.time_of_last_received_clock_message = None

    def get_is_paused(self):
        if not self.any_clock_messages_received:
            return True
        
        seconds_since_last_clock_message = time.time() - self.time_of_last_received_clock_message
        if seconds_since_last_clock_message > NO_CLOCK_MESSAGE_AUTO_PAUSE_TIMEOUT_SECONDS:
            return True
        
        return self.is_paused

    def get_current_time_scale(self):
        if self.get_is_paused():
            return 0.0

        if self.use_time_scale_from_topic:
            return self.time_scale_from_topic

        return self.approximate_timescale

    def get_current_clock_time(self) -> rospy.Time:
        if len(self.timings) == 0:
            return rospy.Time()
        
        newest_timing : ClockTiming = self.timings[-1]
        return newest_timing.clock_time

    def update_estimated_timescale(self):
        if len(self.timings) <= 1:
            self.approximate_timescale = DEFAULT_TIME_SCALE
            self.should_reset_clock_time = True
            return

        self.should_reset_clock_time = False
        
        oldest_timing : ClockTiming = self.timings[0]
        newest_timing : ClockTiming = self.timings[-1]
        
        wall_time_difference_total_seconds = newest_timing.wall_time - oldest_timing.wall_time

        clock_time_difference = newest_timing.clock_time - oldest_timing.clock_time
        clock_time_difference_total_seconds = clock_time_difference.to_sec()

        self.approximate_timescale = clock_time_difference_total_seconds / wall_time_difference_total_seconds

    def on_new_entry_received(self, clock_time: rospy.Time):

        wall_time = time.time()

        if self.use_time_scale_from_topic:
            # Check whether we are moving back to approximate time.
            time_since_last_time_scale_message_seconds = time.time() - self.time_of_last_received_time_scale
            if time_since_last_time_scale_message_seconds > NO_TIMESCALE_MESSAGE_TIMEOUT_SECONDS:
                rospy.loginfo("Moving back to approximate time scale. time_since_last_time_scale_message_seconds = {}".format(time_since_last_time_scale_message_seconds))
                self.use_time_scale_from_topic = False

        if len(self.timings) > 0:

            most_recent_timing: ClockTiming = self.timings[-1]
            clock_time_since_last_message : rospy.Duration = clock_time - most_recent_timing.clock_time

            if clock_time_since_last_message.to_sec() < 0:
                # A jump back in time usually indicates a new ros bag has started playing.
                rospy.loginfo("Jump backward in time detected, clearing all timings. clock_time_since_last_message.to_sec() = {}".format(clock_time_since_last_message.to_sec()))
                self.timings.clear()
            elif clock_time_since_last_message.is_zero():
                # Duplicate time messages to indicate time has stopped.
                rospy.loginfo("Duplicate time message received, Assuming time paused.")
                self.is_paused = True
                return
            else:
                wall_time_since_last_clock_message_seconds = time.time() - self.timings[-1].wall_time
                # Check whether we should clear all stored timings as it's been a while since a stored message.
                # Also, If it was previously paused, then unpause it and clear all timings.
                if wall_time_since_last_clock_message_seconds > CLEAR_HISTORY_TIMEOUT or self.is_paused:
                    rospy.loginfo("Clearing all timings. self.is_paused = {}, wall_time_since_last_clock_message_seconds = {}".format(self.is_paused, wall_time_since_last_clock_message_seconds))
                    self.timings.clear()

                self.is_paused = False

        clock_timing = ClockTiming(wall_time, clock_time)

        if len(self.timings) >= TIMING_HISTORY_COUNT:
            del self.timings[0]

        self.timings.append(clock_timing)

        self.update_estimated_timescale()

        self.any_clock_messages_received = True
        self.time_of_last_received_clock_message = time.time()

    def on_time_scale_from_topic_received(self, time_scale: float):

        time_scale_changed = (not self.use_time_scale_from_topic) or (self.time_scale_from_topic != time_scale)

        self.time_scale_from_topic = time_scale
        self.time_of_last_received_time_scale = time.time()
        self.use_time_scale_from_topic = True

        return time_scale_changed