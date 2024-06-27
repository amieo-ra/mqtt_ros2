#!/usr/bin/env python

import os
import sys
import paho.mqtt.client as mqtt
import json, msgpack, yaml

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, DurabilityPolicy, ReliabilityPolicy, HistoryPolicy
from rclpy.action import ActionClient

from rclpy_message_converter import message_converter
from tf2_msgs.msg import TFMessage
#from ros2_message_converter import message_converter

from std_msgs.msg import String, Float32
import nav_msgs.msg
from nav_msgs.msg import Odometry
#import topological_navigation_msgs.msg
#import toplogical_nav_msgs.msg
from geometry_msgs.msg import Pose

#from strands_navigation_msgs.msg import ExecutePolicyModeActionGoal, ExecutePolicyModeActionFeedback, ExecutePolicyModeActionResult # not sure if this is still needed..
from topological_navigation_msgs.msg import ExecutePolicyModeGoal
from topological_navigation_msgs.action import GotoNode, ExecutePolicyMode
from actionlib_msgs.msg import GoalID, GoalStatusArray

from gofar_navigation_msgs.msg import NewAgentConfigGoF as NewAgentConfig


class MqttPseudoBridge(Node):

  def __init__(self):
    super().__init__('mqtt_pseudo_bridge') #should this be 'mpb'?
    self.logger = self.get_logger()
    # Define all the details for the MQTT broker
    self.mqtt_ip = os.getenv('MQTT_BROKER_IP', 'mqtt.lcas.group')
    self.mqtt_port = int(os.getenv('MQTT_BROKER_PORT', 1883))
    self.mqtt_encoding = os.getenv('MQTT_ENCODING', 'msgpack')

    # Specify the loading and dumping functions
    self.dumps = msgpack.dumps if self.mqtt_encoding == 'msgpack' else json.dumps
    self.loads = msgpack.loads if self.mqtt_encoding == 'msgpack' else json.loads

    # Define source information
    self.robot_name = os.getenv('ROBOT_NAME', '')
    if self.robot_name.startswith('mobile_server'): 
      self.robot_name = ''

    self.source = 'robot' if self.robot_name else 'server'
    self.local_namespace = 'namespace_'+self.source

    self.action_server_name = '/topological_navigation'

    # self.qos = QoSProfile(depth=1, 
    #                       reliability=ReliabilityPolicy.RELIABLE,
    #                       history=HistoryPolicy.KEEP_LAST,
    #                       durability=DurabilityPolicy.TRANSIENT_LOCAL)

    self.client = ActionClient(self, GotoNode, self.action_server_name) # callback_group=self.callback_goto_client)

    # Define topics to connect with
    self.load_topics()

    self.mqtt_topics = dict()
    self.ros_topics = dict()
    self.agents = []

    # Initiate connections to ROS and MQTT
    self.connect_to_mqtt()
    self.connect_to_ros()

  def load_topics(self):
    filename = os.path.dirname(os.path.realpath(__file__)) + "/../config/bridged_topics.yaml"
    topics = open(filename).read().replace("${ROBOT_NAME}", self.robot_name)
    self.topics = yaml.safe_load(topics)

  def connect_to_mqtt(self):
    # MQTT management functions
    self.mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1,
                                   self.source + "_" + self.robot_name)
    self.mqtt_client.on_connect = self.on_connect
    self.mqtt_client.on_message = self.on_message
    self.mqtt_client.connect_async(self.mqtt_ip, self.mqtt_port)
    self.mqtt_client.loop_start()

  def connect_to_ros(self):
    # Define publishers and subscribers to ROS
    if self.source == 'server':
      # On the server, everything not agent-specific is already being published/subscribed
      # so we only need to subscribe to the agent namespace topics, but need the name to do so
      #self.agent_sub = rospy.Subscriber('/rasberry_coordination/dynamic_fleet/add_agent', NewAgentConfig, self.agent_cb)
      
      qos = QoSProfile(depth=1, 
                       reliability=ReliabilityPolicy.RELIABLE,
                       history=HistoryPolicy.KEEP_LAST,
                       durability=DurabilityPolicy.TRANSIENT_LOCAL)
      self.agent_sub = self.create_subscription(NewAgentConfig, '/rasberry_coordination/dynamic_fleet/add_agent', self.agent_cb, qos)
      return

    elif self.source == 'robot':
      print("source is robot")
      # On the robot, everything must be subscribed/published to
      for topic, topic_details in self.topics.items():
        self.make_topic_connection(topic, topic_details, replace_with="/")

  def on_connect(self, client, userdata, flags, rc):
    # When MQTT connects, subscribe to all relevant MQTT topics
    print(" MQTT ->     | Connected")
    for topic, topic_details in self.topics.items():
      # Do not subscribe to add_agent topic, this is handled elsewhere
      if topic == 'rasberry_coordination/dynamic_fleet/add_agent': continue

      # Do not subscribe to self
      if topic_details['source'] == self.source: continue

      # Do not subscribe if topic uses agent namespacing
      if topic_details['namespace_server'] == '/<<rn>>/': continue

      # Subscribe mqtt stream to this topic
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
    data = self.loads(msg.payload)

    rosmsg = topic_info['type_'+self.source]

    #####added#####

    if msg.topic == 'topological_navigation/goal':
      print("set up the action here..!")
      navgoal = GotoNode.Goal()
      navgoal.target = self.nav_client_feedback.marker_name
      navgoal.no_orientation = True 
      self.goal = navgoal
      send_goal_future = self.client.send_goal_async(self.goal,  feedback_callback=self.feedback_callback)

    else:  ####indented
    
      if msg.topic != 'topological_map_2':
        print(" MQTT -> ROS |     payload: "+str(data))
        rosmsg_data = message_converter.convert_dictionary_to_ros_message(rosmsg, data)
        
        #print(rosmsg_data)
      else:
        rosmsg_data = String(data=str(data))
  
      # Publish msg to relevant ROS topic
      rostopic = topic_info['namespace_server'].replace('<<rn>>', sender) + topic
      print(rostopic)
      print(self.ros_topics)
      print(self.ros_topics[rostopic])
      self.ros_topics[rostopic].publish(rosmsg_data)

  def feedback_callback(self, feedback_msg):
    self.nav_client_feedback = feedback_msg.feedback
    self.logger.info("feedback: {} ".format(self.nav_client_feedback))
    return 

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
    #self.logger.info('I heard: "%s"' & msg.pose)
    data = bytearray(self.dumps(message_converter.convert_ros_message_to_dictionary(msg)))    #TODO: ONLY use bytearray is msgpack is being used....
    self.mqtt_client.publish(callback_args, data)

  def make_topic_connection(self, topic, topic_details, replace_with, sub_to_mqtt=False):
    # Define relevent topics
    mqtt_topic = topic_details['namespace_mqtt'].replace('<<rn>>/', replace_with) + topic
    print("mqtt_topic is:", mqtt_topic)
    ros_topic = topic_details[self.local_namespace].replace('<<rn>>/', replace_with) + topic
    print("ros_topic is:", ros_topic)

    # Subscribing to MQTT messages (to publish to MQTT later)
    # On the robot, if the source is the server, we sub to MQTT and pub through to ROS
    # On the server, if the source is the robot, we sub to MQTT and pub through to ROS
    if topic_details['source'] != self.source:
      print(" MQTT -> ROS | publishing from " + mqtt_topic + " to " + ros_topic)
      print(topic_details['type'])
      self.ros_topics[ros_topic] = self.create_publisher(getattr(sys.modules[__name__], topic_details['type']), ros_topic, 10) ####
      if sub_to_mqtt:
        self.mqtt_topics[mqtt_topic] = topic
        self.mqtt_client.subscribe(mqtt_topic)

    # Subscribing to ROS messages (to publish to MQTT later)
    # On the server, if the source is the server, we sub to ROS and pub through to MQTT
    # On the  robot, if the source is the  robot, we sub to ROS and pub through to MQTT
    if topic_details['source'] == self.source:

      qos = QoSProfile(depth=1, 
                       reliability=ReliabilityPolicy.RELIABLE,
                       history=HistoryPolicy.KEEP_LAST,
                       durability=DurabilityPolicy.VOLATILE)
      print(" ROS -> MQTT | publishing from " + ros_topic + " to " + mqtt_topic)
      print("topic info:", topic_details['type'], ros_topic)
      self.callback_args = mqtt_topic
      self.ros_topics[ros_topic] = self.create_subscription(getattr(sys.modules[__name__], topic_details['type']), ros_topic, lambda msg:self.ros_cb(msg, callback_args=mqtt_topic), qos)#, self.ros_cb(callback_args=mqtt_topic), 10) # qos)#, callback_args=mqtt_topic)#, qos)

  def execute(self):     
    if not self.client.server_is_ready():
      self.logger.info("Waiting for the action server {}...".format(self.action_server_name))
      self.client.wait_for_server(timeout_sec=2)
    
    if not self.client.server_is_ready():
      self.logger.info("action server  {} not responding ... can not perform any action".format(self.action_server_name))
      return 
    
    self.logger.info("Executing the action...")
    send_goal_future = self.client.send_goal_async(self.goal,  feedback_callback=self.feedback_callback)
    while rclpy.ok():
      try:
        rclpy.spin_once(self, executor=self.executor_goto_client)
        # rclpy.spin_until_future_complete(self, send_goal_future, executor=self.executor_goto_client, timeout_sec=2.0)
        if send_goal_future.done():
          self.goal_handle = send_goal_future.result()
          break
      except Exception as e:
        self.logger.error("Error while sending the goal to GOTO node {} ".format(e))
        return False  

    if not self.goal_handle.accepted:
      self.logger.error('GOTO action is rejected')
      return False

    self.logger.info('The goal accepted')
    self.goal_get_result_future = self.goal_handle.get_result_async()
    self.logger.info("Waiting for {} action to complete".format(self.action_server_name))
    while rclpy.ok():
      try:
        rclpy.spin_once(self, timeout_sec=0.2)
        if(self.early_terminate_is_required):
          self.logger.warning("Not going to wait till finishing ongoing task, early termination is required ") 
          return False 
        if self.goal_get_result_future.done():
          status = self.goal_get_result_future.result().status
          self.action_status = status
          self.logger.info("Executing the action response with status {}".format(self.get_status_msg(self.action_status)))
          return True 
      except Exception as e:
        self.logger.error("Error while executing go to node policy {} ".format(e))
        # self.goal_get_result_future = None
        return False  


def main(args=None):
  rclpy.init(args=args)
  mqtt_pseudo_bridge = MqttPseudoBridge()
  rclpy.spin(mqtt_pseudo_bridge)
  mqtt_pseudo_bridge.destroy_node()
  rclpy.shutdown()


if __name__ == '__main__':
  main()
