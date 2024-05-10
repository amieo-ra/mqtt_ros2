#!/usr/bin/env python

import os
import paho.mqtt.client as mqtt
import json, msgpack, yaml

import rclpy

from rclpy.node import Node
from rclpy.qos import QoSProfile, DurabilityPolicy

#from rospy_message_converter import message_converter

from rclpy_message_converter import message_converter
#from ros2_message_converter import message_converter

from std_msgs.msg import String, Float32
import nav_msgs.msg
from nav_msgs.msg import Odometry
#import topological_navigation_msgs.msg
#import toplogical_nav_msgs.msg
from geometry_msgs.msg import Pose

#from strands_navigation_msgs.msg import ExecutePolicyModeActionGoal, ExecutePolicyModeActionFeedback, ExecutePolicyModeActionResult # not sure if this is still needed..
#from topological_navigation_msgs.msg import GotoNodeActionGoal, GotoNodeActionFeedback, GotoNodeActionResult
from topological_navigation_msgs.action import GotoNode, ExecutePolicyMode
from actionlib_msgs.msg import GoalID, GoalStatusArray

#try:
from rasberry_coordination.msg import NewAgentConfig   

#except:
    #from gofar_navigation.msg import NewAgentConfig #COMMENTED OUT AS NOT SURE OF THE LOCATION OF THIS..


class MqttPsuedoBridge(Node):

    def __init__(self):
        super().__init__('mqtt_psuedo_bridge') #should this be 'mpb'?
        # Define all the details for the MQTT broker
        self.mqtt_ip = os.getenv('MQTT_BROKER_IP', '10.2.0.1')
        self.mqtt_port = int(os.getenv('MQTT_BROKER_PORT', 8883))
        self.mqtt_encoding = os.getenv('MQTT_ENCODING', 'msgpack')
        mqtt_client = None

        # Specify the loading and dumping functions
        self.dumps = msgpack.dumps if self.mqtt_encoding == 'msgpack' else json.dumps
        self.loads = msgpack.loads if self.mqtt_encoding == 'msgpack' else json.loads

        # Define source information
        self.robot_name = os.getenv('ROBOT_NAME', '')
        if self.robot_name.startswith('mobile_server'): 
            self.robot_name=''

        self.source = 'robot' if self.robot_name else 'server'
        self.local_namespace = 'namespace_'+self.source



        # Define topics to connect with (TODO: move this to read from yaml config file)
        self.topics = {
            'topological_map_2': {
                'source':'server',
                'namespace_robot': '/',
                'namespace_mqtt': '',
                'namespace_server': '/',
                #'namespace_server': '/restricted_topological_map_generators/',
                'type_robot': 'std_msgs/String',
                'type_server': 'std_msgs/String',  #There is some bug with this part, data no exist?
                'type': String
            },
#            'rasberry_coordination/dynamic_fleet/add_agent': { # commenting out as this hasn't been transferred across to ROS2 hunter yet..
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': '',
#                'namespace_server': '/',
#                'type_robot': 'gofar_navigation/NewAgentConfig',
#                'type_server': 'rasberry_coordination/NewAgentConfig',
#                'type': NewAgentConfig
#            },
#            'topological_navigation/execute_policy_mode/goal': { # commented out for testing
#                'source':'server',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'topological_navigation_msgs/ExecutePolicyModeActionGoal',
#                'type_server': 'strands_navigation_msgs/ExecutePolicyModeActionGoal',
#                'type': ExecutePolicyModeActionGoal
#            },
            'topological_navigation/execute_policy_mode/cancel': {
                'source':'server',
                'namespace_robot': '/',
                'namespace_mqtt': self.robot_name+'<<rn>>/',
                'namespace_server': '/'+self.robot_name+'<<rn>>/',
                'type_robot': 'actionlib_msgs/GoalID',
                'type_server': 'actionlib_msgs/GoalID',
                'type': GoalID
            },
#            'topological_navigation/goal': { #commented out just for testing
#                'source':'server',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'topological_navigation_msgs/GotoNodeActionGoal',
#                'type_server': 'strands_navigation_msgs/GotoNodeActionGoal',
#                'type': GotoNodeActionGoal
#            },
            'topological_navigation/cancel': {
                'source':'server',
                'namespace_robot': '/',
                'namespace_mqtt': self.robot_name+'<<rn>>/',
                'namespace_server': '/'+self.robot_name+'<<rn>>/',
                'type_robot': 'actionlib_msgs/GoalID',
                'type_server': 'actionlib_msgs/GoalID',
                'type': GoalID
            },
#            'ui/speaker': { # commenting out as this doesn't exist in ROS2 hunter
#                'source':'server',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'std_msgs/String',
#                'type_server': 'std_msgs/String',
#                'type': String
#            },
            'current_node': {
                'source':'robot',
                'namespace_robot': '/',
                'namespace_mqtt': self.robot_name+'<<rn>>/',
                'namespace_server': '/'+self.robot_name+'<<rn>>/',
                'type_robot': 'std_msgs/String',
                'type_server': 'std_msgs/String',
                'type': String
            },
            'closest_node': {
                'source':'robot',
                'namespace_robot': '/',
                'namespace_mqtt': self.robot_name+'<<rn>>/',
                'namespace_server': '/'+self.robot_name+'<<rn>>/',
                'type_robot': 'std_msgs/String',
                'type_server': 'std_msgs/String',
                'type': String
            },
#            'topological_navigation/execute_policy_mode/feedback': {
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'topological_navigation_msgs/ExecutePolicyModeActionFeedback',
#                'type_server': 'strands_navigation_msgs/ExecutePolicyModeActionFeedback',
#                'type': ExecutePolicyModeActionFeedback
#            },
#            'topological_navigation/execute_policy_mode/result': {
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'topological_navigation_msgs/ExecutePolicyModeActionResult',
#                'type_server': 'strands_navigation_msgs/ExecutePolicyModeActionResult',
#                'type': ExecutePolicyModeActionResult
#            },
#            'topological_navigation/execute_policy_mode/status': {
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'actionlib_msgs/GoalStatusArray',
#                'type_server': 'actionlib_msgs/GoalStatusArray',
#                'type': GoalStatusArray
#            },
#            'topological_navigation/feedback': {
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'topological_navigation_msgs/GotoNodeActionFeedback',
#                'type_server': 'strands_navigation_msgs/GotoNodeActionFeedback',
#                'type': GotoNodeActionFeedback
#            },
#            'topological_navigation/result': {
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'topological_navigation_msgs/GotoNodeActionResult',
#                'type_server': 'strands_navigation_msgs/GotoNodeActionResult',
#                'type': GotoNodeActionResult
#            },
#            'topological_navigation/status': {
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'actionlib_msgs/GoalStatusArray',
#                'type_server': 'actionlib_msgs/GoalStatusArray',
#                'type': GoalStatusArray
#            },
#            'han/balance_data': { # commented out as this doesn't exist in ROS2 hunter..
#                'source':'robot',
#                'namespace_robot': '/',
#                'namespace_mqtt': self.robot_name+'<<rn>>/',
#                'namespace_server': '/'+self.robot_name+'<<rn>>/',
#                'type_robot': 'std_msgs/Float32',
#                'type_server': 'std_msgs/Float32',
#                'type': Float32
#            },
#            'robot_pose': {
            'odometry/global': {
                'source':'robot',
                'namespace_robot': '/',
                'namespace_mqtt': self.robot_name+'<<rn>>/',
                'namespace_server': '/'+self.robot_name+'<<rn>>/',
                'type_robot': 'geometry_msgs/Pose',
                'type_server': 'geometry_msgs/Pose',
                'type': Odometry
            },
        }

        self.mqtt_topics = dict()
        self.ros_topics = dict()
        self.agents = []

        # Initiate connections to ROS and MQTT
        self.connect_to_mqtt()
        self.connect_to_ros()


    def connect_to_mqtt(self):
        # MQTT management functions
        self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, self.source + "_" + self.robot_name)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        #self.mqtt_client.connect(self.mqtt_ip, self.mqtt_port)
        self.mqtt_client.connect_async(self.mqtt_ip, self.mqtt_port)
        self.mqtt_client.loop_start()


    def on_connect(self, client, userdata, flags, rc):
        # When MQTT connects, subscribe to all relevant MQTT topics
        print(" MQTT ->     | Connected")
        for topic, topic_details in self.topics.items():

            # We dont want to subscribe to the add_agent topic, this is handled elsewhere
            if topic == 'rasberry_coordination/dynamic_fleet/add_agent': continue

            # We dont want to subscribe if the message it local
            if topic_details['source'] == self.source: continue

            # Skip if topic uses agent namespacing
            if topic_details['namespace_server'] == '/<<rn>>/': continue

            #Subscribe mqtt stream to this topic
            mqtt_topic = topic_details['namespace_mqtt'].replace('<<rn>>/', '/') + topic
            print(" MQTT ->     | subscribing to " + mqtt_topic)
            self.mqtt_topics[mqtt_topic] = topic
            self.mqtt_client.subscribe(mqtt_topic)
            #TODO: we could skip this and only connect on agent load? for the agent-specific topics only?

    def on_message(self, client, userdata, msg):
        # Identify topic
        print('\n\n')
        print(" MQTT        | Message received ["+msg.topic+"]")

        def remove_suffix(text, suffix):
            if text.endswith(suffix):
                return text[:len(text)-len(suffix)]
            return text

        # Get details of topic
        topic = self.mqtt_topics[msg.topic]
        sender = remove_suffix(msg.topic, topic)
        sender = remove_suffix(sender, '/')
        topic_info = self.topics[topic]

        print(topic)
        print(sender)

        #print(topic_info)
        msg_type = topic_info['type']

        #Convert bytearray to msg
        msg = self.loads(msg.payload)

        rosmsg = topic_info['type_'+self.source]

        if msg.topic != 'topological_map_2':
            print(" MQTT -> ROS |     payload: "+str(msg))
            rosmsg_data = message_converter.convert_dictionary_to_ros_message(rosmsg, msg)
            #print(rosmsg_data)
        else:
            rosmsg_data = str(msg)

        # Publish msg to relevant ROS topic
        #print(self.ros_topics)
        rostopic =  topic_info['namespace_server'].replace('<<rn>>', sender) + topic
        #print(rostopic)
        self.ros_topics[rostopic].publish(rosmsg_data)


    def connect_to_ros(self):
        # Define publishers and subscribers to ROS
        if self.source == 'server':
            # On the server, everything not agent-specific is already being published/subscribed
            # so we only need to subscribe to the agent namespace topics, but need the name to do so
            #self.agent_sub = rospy.Subscriber('/rasberry_coordination/dynamic_fleet/add_agent', NewAgentConfig, self.agent_cb)
            self.agent_sub = self.create_subscription(NewAgentConfig, '/rasberry_coordination/dynamic_fleet/add_agent', self.agent_cb)
            return

        elif self.source == 'robot':
            print("source is robot")
            # On the robot, everything must be subscribed/published to
            for topic, topic_details in self.topics.items():
                self.make_topic_connection(topic, topic_details, replace_with="/")

    def agent_cb(self, msg):
        # Skip if agent exists
        if msg.agent_id in self.agents: return
        self.agents += [msg.agent_id]
        print('       AGENT | New agent detected: ' + msg.agent_id)

        # Otherwise subscribe
        for topic, topic_details in self.topics.items():

            # Skip if topic doesnt use agent namespacing
            if topic_details['namespace_server'] == '/': continue

            # Construct topics
            self.make_topic_connection(topic, topic_details, replace_with=msg.agent_id+'/', sub_to_mqtt=True)

    # Our subscribers to get data from the local ROS and into the MQTT broker
    def ros_cb(self, msg, callback_args):
        print(" MQTT        | publishing on "+callback_args)
        data = bytearray(self.dumps(message_converter.convert_ros_message_to_dictionary(msg)))    #TODO: ONLY use bytearray is msgpack is being used....
        self.mqtt_client.publish(callback_args, data)



    def make_topic_connection(self, topic, topic_details, replace_with, sub_to_mqtt=False):
        # Define relevent topics
        mqtt_topic = topic_details['namespace_mqtt'].replace('<<rn>>/', replace_with) + topic
        ros_topic = topic_details[self.local_namespace].replace('<<rn>>/', replace_with) + topic

        # Subscribing to MQTT messages (to publish to MQTT later)
        # On the robot, if the source is the server, we sub to MQTT and pub through to ROS
        # On the server, if the source is the robot, we sub to MQTT and pub through to ROS
        if topic_details['source'] != self.source:
            print(" MQTT -> ROS | publishing from " + mqtt_topic + " to " + ros_topic)
            #self.ros_topics[ros_topic] = rospy.Publisher(ros_topic, topic_details['type'], queue_size=10)
            self.ros_topics[ros_topic] = self.create_publisher(topic_details['type'], ros_topic, 10) ####
            if sub_to_mqtt:
                self.mqtt_topics[mqtt_topic] = topic
                self.mqtt_client.subscribe(mqtt_topic)

        # Subscribing to ROS messages (to publish to MQTT later)
        # On the server, if the source is the server, we sub to ROS and pub through to MQTT
        # On the  robot, if the source is the  robot, we sub to ROS and pub through to MQTT
        if topic_details['source'] == self.source:
            print(" ROS -> MQTT | publishing from " + ros_topic + " to " + mqtt_topic)
            #qos = QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL)
            qos = QoSProfile(depth=1, durability=DurabilityPolicy.VOLATILE)
            #self.ros_topics[ros_topic] = rospy.Subscriber(ros_topic, topic_details['type'], self.ros_cb, callback_args=mqtt_topic)
            print("topic info:", topic_details['type'], ros_topic)
            self.ros_topics[ros_topic] = self.create_subscription(topic_details['type'], ros_topic, self.ros_cb(callback_args=mqtt_topic), 10) # qos)#, callback_args=mqtt_topic)#, qos)
            self.ros_topics[ros_topic]
        

if __name__ == '__main__':
    rclpy.init(args=None)
    mqtt_psuedo_bridge = MqttPsuedoBridge()
    rclpy.spin(mqtt_psuedo_bridge)


    mqtt_psuedo_bridge.destroy_node()
    rclpy.shutdown()
