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
import logging
import json
import sys
import threading
import importlib

from std_msgs.msg import Time, Float32
from rosgraph_msgs.msg import Clock

from .tcp_sender import UnityTcpSender
from .client import ClientThread
from .subscriber import RosSubscriber
from .publisher import RosPublisher
from .service import RosService
from .unity_service import UnityService
from .ros_clock import ClockTimings


class TcpServer:
    """
    Initializes ROS node and TCP server.
    """

    def __init__(self, node_name, buffer_size=1024, connections=10, tcp_ip=None, tcp_port=None):
        """
        Initializes ROS node and class variables.

        Args:
            node_name:               ROS node name for executing code
            buffer_size:             The read buffer size used when reading from a socket
            connections:             Max number of queued connections. See Python Socket documentation
        """
        if tcp_ip:
            self.loginfo("Using 'tcp_ip' override from constructor: {}".format(tcp_ip))
            self.tcp_ip = tcp_ip
        else:
            self.tcp_ip = rospy.get_param("~tcp_ip", "0.0.0.0")

        if tcp_port:
            self.loginfo("Using 'tcp_port' override from constructor: {}".format(tcp_port))
            self.tcp_port = tcp_port
        else:
            self.tcp_port = rospy.get_param("~tcp_port", 10000)

        self.unity_tcp_sender = UnityTcpSender(self)

        self.node_name = node_name
        self.publishers_table = {}
        self.subscribers_table = {}
        self.ros_services_table = {}
        self.unity_services_table = {}
        self.buffer_size = buffer_size
        self.connections = connections
        self.syscommands = SysCommands(self)
        self.pending_srv_id = None
        self.pending_srv_is_request = False

        self.use_sim_time = rospy.get_param("/use_sim_time", False)
        if not self.use_sim_time:
            # Start Subscriber listener function
            rospy.loginfo("use_sim_time is false, assuming ros wall time will be used.")
            # TODO - Implement.
            self.clock_timings = ClockTimings()
            self.clock_sub = rospy.Subscriber('/clock', Clock, self.on_clock_received)
            self.time_scale_sub = rospy.Subscriber('/time_scale', Float32, self.on_time_scale_received)

    def on_clock_received(self, clock: Clock):
        self.clock_timings.on_new_entry_received(clock.clock)
        self.send_clock_info(self.clock_timings)

    def on_time_scale_received(self, time_scale: Float32):
        should_send_updated_clock_info = self.clock_timings.on_time_scale_from_topic_received(time_scale.data)
        if should_send_updated_clock_info:
            self.send_clock_info(self.clock_timings)

    def start(self, publishers=None, subscribers=None):
        if publishers is not None:
            self.publishers_table = publishers
        if subscribers is not None:
            self.subscribers_table = subscribers
        server_thread = threading.Thread(target=self.listen_loop)
        # Exit the server thread when the main thread terminates
        server_thread.daemon = True
        server_thread.start()

    def listen_loop(self):
        """
            Creates and binds sockets using TCP variables then listens for incoming connections.
            For each new connection a client thread will be created to handle communication.
        """
        self.loginfo("Starting server on {}:{}".format(self.tcp_ip, self.tcp_port))
        tcp_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        tcp_server.bind((self.tcp_ip, self.tcp_port))

        while True:
            tcp_server.listen(self.connections)

            try:
                (conn, (ip, port)) = tcp_server.accept()
                ClientThread(conn, self, ip, port).start()
            except socket.timeout as err:
                self.logerr("ros_tcp_endpoint.TcpServer: socket timeout")

    def send_unity_error(self, error):
        self.unity_tcp_sender.send_unity_error(error)

    def send_service_failure(self, srv_id, error_message):
        self.unity_tcp_sender.send_service_failure(srv_id, error_message)

    def send_service_error(self, srv_id, error_message):
        self.unity_tcp_sender.send_service_error(srv_id, error_message)

    def send_unity_message(self, topic, message):
        self.unity_tcp_sender.send_unity_message(topic, message)

    def send_unity_service(self, topic, service_class, request):
        return self.unity_tcp_sender.send_unity_service_request(topic, service_class, request)

    def send_unity_service_response(self, srv_id, data):
        self.unity_tcp_sender.send_unity_service_response(srv_id, data)

    def send_clock_info(self, clock_timings: ClockTimings):
        self.unity_tcp_sender.send_clock_info(
            clock_timings.get_current_clock_time(), 
            clock_timings.get_current_time_scale(), 
            clock_timings.is_paused,
            clock_timings.should_reset_clock_time)

    def handle_syscommand(self, topic, data):
        try:
            function = getattr(self.syscommands, topic[2:])
        except:
            function = None
        if function is None:
            self.send_unity_error("Don't understand SysCommand.'{}'".format(topic))
        else:
            message_json = data.decode("utf-8")
            params = json.loads(message_json)
            function(**params)

    def loginfo(self, text):
        rospy.loginfo(text)

    def logwarn(self, text):
        rospy.logwarn(text)

    def logerr(self, text):
        rospy.logerr(text)

    def unregister_node(self, old_node):
        if old_node is not None:
            old_node.unregister()


class SysCommands:
    def __init__(self, tcp_server):
        self.tcp_server = tcp_server

    def subscribe(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't subscribe to a blank topic name! SysCommand.subscribe({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.subscribe - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.subscribers_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)
            self.tcp_server.loginfo("Unregistering old Subscriber({}, {}) OK".format(topic, message_class))

        new_subscriber = RosSubscriber(topic, message_class, self.tcp_server)
        self.tcp_server.subscribers_table[topic] = new_subscriber

        self.tcp_server.loginfo("RegisterSubscriber({}, {}) OK".format(topic, message_class))

    def remove_subscriber(self, topic):
        if topic not in self.tcp_server.subscribers_table:
            self.tcp_server.send_unity_error(
                "Unable to remove subscriber {} as is it not known!".format(
                    topic
                )
            )
            return
        
        # Unregister the subscribed topic.
        subscriber_to_remove = self.tcp_server.subscribers_table[topic]
        subscriber_to_remove.unregister()
        del self.tcp_server.subscribers_table[topic]

        self.tcp_server.loginfo("UnregisterSubscriber({}) OK".format(topic))

    def publish(self, topic, message_name, queue_size=10, latch=False):
        if topic == "":
            self.tcp_server.send_unity_error(
                "Can't publish to a blank topic name! SysCommand.publish({}, {})".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name)
        if message_class is None:
            self.tcp_server.send_unity_error(
                "SysCommand.publish - Unknown message class '{}'".format(message_name)
            )
            return

        old_node = self.tcp_server.publishers_table.get(topic)
        # I'm not sure whether unregister_node needs to be called all the time.
        # My best guess is to account for a new message_class on the same topic.
        # However the it may be contributing to the open file leak problem.
        # Perhaps we should only be doing this if message_class, queue_size or latch has changed.
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_publisher = RosPublisher(topic, message_class, queue_size=queue_size, latch=latch)

        self.tcp_server.publishers_table[topic] = new_publisher

        self.tcp_server.loginfo("RegisterPublisher({}, {}) OK".format(topic, message_class))

    def ros_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return
        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterRosService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.ros_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = RosService(topic, message_class)

        self.tcp_server.ros_services_table[topic] = new_service

        self.tcp_server.loginfo("RegisterRosService({}, {}) OK".format(topic, message_class))

    def unity_service(self, topic, message_name):
        if topic == "":
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Can't register a blank topic name!".format(
                    topic, message_name
                )
            )
            return

        message_class = self.resolve_message_name(message_name, "srv")
        if message_class is None:
            self.tcp_server.send_unity_error(
                "RegisterUnityService({}, {}) - Unknown service class '{}'".format(
                    topic, message_name, message_name
                )
            )
            return

        old_node = self.tcp_server.unity_services_table.get(topic)
        if old_node is not None:
            self.tcp_server.unregister_node(old_node)

        new_service = UnityService(str(topic), message_class, self.tcp_server)

        self.tcp_server.unity_services_table[topic] = new_service

        self.tcp_server.loginfo("RegisterUnityService({}, {}) OK".format(topic, message_class))

    def response(self, srv_id):  # the next message is a service response
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = False

    def request(self, srv_id):  # the next message is a service request
        self.tcp_server.pending_srv_id = srv_id
        self.tcp_server.pending_srv_is_request = True

    def topic_list(self):
        self.tcp_server.unity_tcp_sender.send_topic_list()

    def ping(self, request_time):
        self.tcp_server.unity_tcp_sender.send_ping_response(request_time)

    def resolve_message_name(self, name, extension="msg"):
        try:
            if len(name) < 1:
                self.tcp_server.logerr(
                    "Failed to resolve empty message name; if message is being registered before Start(), please specify ROS message name, or move registration to Start()."
                )
                return None
            names = name.split("/")
            module_name = names[0]
            class_name = names[1]
            importlib.import_module(module_name + "." + extension)
            module = sys.modules[module_name]
            if module is None:
                self.tcp_server.logerr("Failed to resolve module {}".format(module_name))
            module = getattr(module, extension)
            if module is None:
                self.tcp_server.logerr(
                    "Failed to resolve module {}.{}".format(module_name, extension)
                )
            module = getattr(module, class_name)
            if module is None:
                self.tcp_server.logerr(
                    "Failed to resolve module {}.{}.{}".format(module_name, extension, class_name)
                )
            return module
        except (IndexError, KeyError, AttributeError, ImportError) as e:
            self.tcp_server.logerr("Failed to resolve message name: {}".format(e))
            return None
