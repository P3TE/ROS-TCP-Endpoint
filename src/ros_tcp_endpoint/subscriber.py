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
import socket

from .communication import RosReceiver
from .client import ClientThread


class RosSubscriber(RosReceiver):
    """
    Class to send messages outside of ROS network
    """

    def __init__(self, topic, message_class, tcp_server, queue_size=10):
        """

        Args:
            topic:         Topic name to publish messages to
            message_class: The message class in catkin workspace
            queue_size:    Max number of entries to maintain in an outgoing queue
        """
        self.topic = topic
        self.node_name = "{}_subscriber".format(topic)
        self.msg = message_class
        self.tcp_server = tcp_server
        self.queue_size = queue_size
        self.socket = None
        self.connected = False
        self.most_recent_exception_message = ""
        self.subscriber = None

        # Start Subscriber listener function
        self.listener()

    def send(self, data):
        """
        Connect to TCP endpoint on client and pass along message
        Args:
            data: message data to send outside of ROS network

        Returns:
            self.msg: The deserialize message

        """
        if rospy.is_shutdown():
            return

        if not self.connected:
            if self.create_new_connection():
                self.connected = True
            else:
                return

        try:
            serialized_message = ClientThread.serialize_message(self.topic, data)
            self.socket.send(serialized_message)
        except Exception as e:
            self.close_connection_if_applicable()
            exception_message = "Exception {}".format(e)
            if not exception_message == self.most_recent_exception_message:
                self.most_recent_exception_message = exception_message
                rospy.loginfo(exception_message)

        return self.msg

    def create_new_connection(self):
        # print("Attempting to creat a new connection for topic {}".format(self.topic))
        if self.tcp_server.unity_tcp_sender.unity_ip == '':
            if not self.tcp_server.unity_tcp_sender.unity_ip_error_message_printed:
                self.tcp_server.unity_tcp_sender.unity_ip_error_message_printed = True
                print("Can't send a message on topic {}, no defined unity IP!".format(self.topic))
            return False

        # print("Creating a new connection")
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(999999)
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.socket.connect((self.tcp_server.unity_tcp_sender.unity_ip, self.tcp_server.unity_tcp_sender.unity_port))
            return True
        except Exception as e:
            exception_message = "Exception {}".format(e)
            if not exception_message == self.most_recent_exception_message:
                self.most_recent_exception_message = exception_message
                rospy.loginfo(exception_message)

        return False

    def listener(self):
        """

        Returns:

        """
        self.subscriber = rospy.Subscriber(self.topic, self.msg, self.send)

    def close_connection_if_applicable(self):
        self.connected = False
        if self.socket is not None:
            try:
                self.socket.close()
            except Exception:
                pass

    def unregister_subscriber(self):
        if self.subscriber is not None:
            self.subscriber.unregister()