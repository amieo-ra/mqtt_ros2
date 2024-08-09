#!/usr/bin/env python

#import tf
import sys
import os
import json, yaml
from pprint import pprint

#import rospy
import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, DurabilityPolicy

#import rospkg
from std_msgs.msg import String
from geometry_msgs.msg import Pose
from diagnostic_msgs.msg import KeyValue

from gofar_navigation_msgs.msg import NewAgentConfigGoF, Module

class addagent(Node):

    def __init__(self):
        super().__init__('AddAgent')
        self.publisher = self.create_publisher(NewAgentConfigGoF, "/rasberry_coordination/dynamic_fleet/add_agent", 10)
        timer_period = 0.25
        self.timer = self.create_timer(timer_period, self.timer_callback)
        self.i = 0
        self.declare_parameter("setupfile", rclpy.Parameter.Type.STRING)
        #self.setupfile = self.get_parameter("setupfile").value

    def timer_callback(self):

        # Collect details
        agent_id = os.getenv('ROBOT_NAME', 'gofar_001')
        setup = '/home/ros/aoc_strawberry_scenario_ws/src/external_packages/mqtt_ROS2/mqtt_ros2/short.yaml'
       
        #setup = os.getenv('AGENT_SETUP_CONFIG', self.setupfile)

        # print("AddAgent Node launched")
        # print("Loading configurations:")
        # print("    - agent_file: %s"%agent_id)
        # print("    - setup_file: %s"%setup)

        agent = self.load_agent_obj(agent_id, setup)
        # print("Details of Agent being launched:\n%s\n\n"%agent)

        self.publisher.publish(agent)


    def get_kvp_list(self, dict, item):
        if item in dict:
            return [KeyValue(k, str(v)) for k, v in dict[item].items()]
        return []


    def load_agent_obj(self, agent_id, setup_file, printer=True):

        # Load file contents, (fallback on empty file if agent_file not found)
        agent_data = {'agent_id': agent_id.split("/")[-1].split(".")[0]}
        # print("Launching with agent_data: %s" % (agent_data))

        # Build msg
        with open(setup_file) as f:
            setup_data = yaml.safe_load(f)
        # pprint(setup_data)
        # print("\n")

        # Construct object
        agent = NewAgentConfigGoF()
        agent.agent_id = agent_data['agent_id']
        agent.local_properties = self.get_kvp_list(agent_data, 'local_properties')
        for m in setup_data['modules']:
            m['details'] = m['details'] if 'details' in m else [{'key':'value'}]



        agent.modules = [Module(name=m['name'],
                                interface=m['interface'], 
                                details=[KeyValue(
                                    key=list(d.keys())[0], 
                                    value=str(yaml.dump(list(d.values())[0])))
                        for d in m['details']]) 
                        for m in setup_data['modules']]
        
        # print("\n\n")
        
        return agent
    
def main(args=None):
    rclpy.init()
    AddAgent = addagent()
    rclpy.spin(AddAgent)

if __name__ == '__main__':
    main()



