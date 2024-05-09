#!/usr/bin/env python3
import os, time
import json, msgpack
from pprint import pprint

import paho.mqtt.client as mqtt

import rclpy
from rclpy.node import Node
from rclpy.qos import QoSProfile, DurabilityPolicy

from std_msgs.msg import String
from diagnostic_msgs.msg import KeyValue
from geometry_msgs.msg import PoseArray, PoseStamped, Pose



# FROM farm_coordinatoin_systems.ffr_utils.ffr_utils.rosmsg.py

def convert_ros_message_to_dictionary(rosmsg):
    """ Recursively break apart the rosmsg into a layered dictionary """
    dict_obj = dict()

    # Loop through each property in the rosmsg
    for property_name, property_type in rosmsg._fields_and_field_types.items():
        property = getattr(rosmsg, property_name)

        # If the property is a list of objects
        if 'sequence' in property_type:
            data = []
            for item in property:
                data += [convert_ros_message_to_dictionary(item)]

        # If the property is another rosmsg object
        elif '/' in property_type:
            data = convert_ros_message_to_dictionary(property)

        # Else the property is a standard type
        else:
            data = property

        # Save the property to the dictionary
        dict_obj[property_name] = data
    return dict_obj

#from ffr_utils.rosmsg import convert_ros_message_to_dictionary as rosdict
rosdict = convert_ros_message_to_dictionary






"""
('topic')           ('by mother')                                                          ('by duckling')
'/advert'         | ('list_as_L') 'ros'> 'on_ROS_ad_ad'    >'M'>>'on_mqtt_message' >>'ros' ('listen_to_L')  |
'/advert_bid'     | ('send_as_L') 'ros'<<'on_mqtt_message'<<'M'< 'on_ROS_ad_bid'    <'ros' ('listen_to_L')  |
'/advert_award'   | ('send_as_L') 'ros'> 'on_ROS_ad_award' >'M'>>'on_mqtt_message' >>'ros' ('listen_to_L')  |
'/advert_reject'  | ('send_as_L') 'ros'<<'on_mqtt_message'<<'M'< 'on_ROS_ad_reject' <'ros' ('listen_to_L')  |
'/safe_node_list' | ('conn_as_L') 'ros'> 'on_ROS_snl'      >'M'>>'on_mqtt_message' >>'ros' ('connect_to_L') |
                      ^ '[In theory, this should not be advertised as default, it may pose a safety issue?]'
"""


class Quacker(Node):

    def __init__(self, robot_name, act_as_mother=True, act_as_duckling=True):
        super().__init__('quacker')

        # Define all the details for the MQTT broker
        self.mqtt_ip = os.getenv('MQTT_BROKER_IP', '')
        self.mqtt_port = int(os.getenv('MQTT_BROKER_PORT', 8883))
        self.mqtt_encoding = os.getenv('MQTT_ENCODING', 'json')
        self.mqtt_client = None

        # Specify the loading and dumping functions
        self.dumps = msgpack.dumps if self.mqtt_encoding == 'msgpack' else json.dumps
        self.loads = msgpack.loads if self.mqtt_encoding == 'msgpack' else json.loads

        # Define source information
        self.robot_name = robot_name
        self.act_as_mother = act_as_mother
        self.act_as_duckling = act_as_duckling

        # Reset and establish the connections
        self.connection_reset()

    def connection_reset(self):
        print('Resetting connections.')

        # Delete any existing connections
        self.mqtt_client = None

        self.ad_ad__ros_sub = None
        self.ad_interest__ros_sub = None
        self.ad_bid__ros_sub = None
        self.ad_award__ros_sub = None
        self.ad_reject__ros_sub = None

        # Initiate connections to ROS and MQTT
        self.connect_to_mqtt()
        self.connect_to_ros()

    def connect_to_mqtt(self):
        # MQTT management functions
        ver = mqtt.CallbackAPIVersion.VERSION1
        self.mqtt_client = mqtt.Client(ver, self.robot_name+'_quacker')
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.connect(self.mqtt_ip, self.mqtt_port)
        self.mqtt_client.loop_start()

    def on_connect(self, client, userdata, flags, rc):
        print(" MQTT ->     | Connected")

    # There are 5 topics in total which communicate between mother and duckling
    #
    # Global Topics:
    #   - /all/duckling/advert
    #
    # Data from Mother to Duckling:
    #   - /mother/duckling/advert_award
    #   - /mother/duckling/safe_node_list
    #
    # Data from Duckling to Mother:
    #   - /mother/duckling/advert_bid
    #   - /mother/duckling/advert_reject
    #
    # Notes:
    #   - The mother does not connect to any duckling topics
    #   - The mother can initialise all publishers when quack is launched
    #   - The mother can begin publishing immediately even if noone listens
    #   - The duckling begins listening to mother when it makes a bid
    #   - Optionally: we could make the duckling only initialise on bid award
    #

    def connect_to_ros(self):

        # The duckling will publish to advert_interest when it begins getting a mother
        # The mother will publish to advert when it begins looking for a follower

        if self.act_as_mother:
            self.listen_as_mother()
            self.connect_as_mother()

        if self.act_as_duckling:
            self.listen_as_duckling()









    ############################
    ## Connecting to MQTT/ROS ##
    ############################
    ## as mother              ##
    ############################

    def listen_as_mother(self):

        # The mother must be able to publish its messages
        # for each channel by default, and without issue
        # this is where these connections are initialised

        #
        ns = 'ducklings'
        t = f'/all/duckling/advert'

        # Mother should publish its advert
        # (publish to mqtt)
        qos = QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL)
        print(f'Subscribing to ROS   |-> {t}')
        print(f'Publishing to MQTT   |-> {ns}{t}')
        self.ad_ad__ros_sub = self.create_subscription(PoseStamped, t, self.on_ROS_ad_ad, qos)
        self.ad_ad_mqtt_pub = lambda mqtt_client, msg : mqtt_client.publish(f'{ns}{t}', msg)

    def connect_as_mother(self):

        # The mother must be able to publish its messages
        # for each channel by default, and without issue
        # this is where these connections are initialised

        #
        ns = 'ducklings'
        t1 = f'/{self.robot_name}/duckling/advert_bid'
        t2 = f'/{self.robot_name}/duckling/advert_award'
        t3 = f'/{self.robot_name}/duckling/advert_reject'

        # Mother should recieve bids from followers
        # (publish to ros)
        print(f'Subscribing to MQTT  |<- {ns}{t1}')
        print(f'Publishing to ROS    |<- {t1}')
        self.ad_bid_mqtt_sub = self.mqtt_client.subscribe(f'{ns}{t1}')
        self.ad_bid__ros_pub = self.create_publisher(KeyValue, t1, 10)

        # Mother should publish its award
        # (publish to mqtt)
        print(f'Subscribing to ROS   |-> {t2}')
        print(f'Publishing to MQTT   |-> {ns}{t2}')
        self.ad_award__ros_sub = self.create_subscription(KeyValue, t2, self.on_ROS_ad_award, 10)
        self.ad_award_mqtt_pub = lambda mqtt_client, msg : mqtt_client.publish(f'{ns}{t2}', msg)

        # Mother should recieve its rejection from follower
        # (publish to ros)
        print(f'Subscribing to MQTT  |<- {ns}{t3}')
        print(f'Publishing to ROS    |<- {t3}')
        self.ad_reject_mqtt_sub = self.mqtt_client.subscribe(f'{ns}{t3}')
        self.ad_reject__ros_pub = self.create_publisher(KeyValue, t3, 10)

    def attach_as_mother(self):

        # Add additional callbacks to occur when the leader has approved the bid from the robot
        ns = 'ducklings'
        t = f'/{self.robot_name}/duckling/safe_node_list'

        # Connect to approved leader to recieve the list of safe nodes
        # (subscribe to mqtt)
        qos = QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL)
        print(f'Subscribing to ROS   |-> {t}')
        print(f'Publishing to MQTT   |-> {ns}{t}')
        self.snl__ros_sub = self.create_subscription(PoseArray, t, self.on_ROS_snl, qos)
        self.snl_mqtt_pub = lambda mqtt_client, msg : mqtt_client.publish(f'{ns}{t}', msg)
    def dettach_as_mother(self):
        pass











    ############################
    ## Connecting to MQTT/ROS ##
    ############################
    ## as duckling            ##
    ############################

    def listen_as_duckling(self):

        # Add additional callbacks to occur when the leader has approved the bid from the robot
        ns = 'ducklings'
        t1 = f'/all/duckling/advert'
        t2 = f'/{self.robot_name}/duckling/advert_interest'

        # Duckling needs to recieve global adverts
        # (subscribe to mqtt)
        qos = QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL)
        print(f'Subscribing to MQTT  |-> {ns}{t1}')
        print(f'Publishing to ROS    |-> {t1}')
        self.ad_ad_mqtt_sub = self.mqtt_client.subscribe(f'{ns}{t1}')
        self.ad_ad__ros_pub = self.create_publisher(PoseStamped, t1, qos)

        # Duckling will publish its interest in an advert
        print(f'Subscribing to ROS   |-> {t2}')
        self.ad_interest__ros_sub = self.create_subscription(String, t2, self.on_ROS_ad_interest, 10)


    def connect_as_duckling(self, msg):

        # The duckling will recieve an advert from the mother and wish to publish a bid to it
        # this system is to recieve the notification of interest and establish the communication
        # connections to facilitate this.
        self.leader = msg.data
        print(f'Request come in locally to listen to: {self.leader}')

        # The bidder subscribes to the appropriate mqtt topics
        ns = 'ducklings'
        t1 = f'/{self.leader}/duckling/advert_bid'
        t2 = f'/{self.leader}/duckling/advert_award'
        t3 = f'/{self.leader}/duckling/advert_reject'

        # Bidder should publish its bid
        # (publish to mqtt)
        print(f'Subscribing to ROS   | {t1}')
        print(f'Publishing to MQTT   | {ns}{t1}')
        self.ad_bid__ros_sub = self.create_subscription(KeyValue, t1, self.on_ROS_ad_bid, 10)
        self.ad_bid_mqtt_pub = lambda mqtt_client, msg : mqtt_client.publish(f'{ns}{t1}', msg)

        # Bidder should subscribe to the leader's award notice
        # (subscribe to ros)
        print(f'Subscribing to MQTT  | {ns}{t2}')
        print(f'Publishing to ROS    | {t2}')
        self.ad_award_mqtt_sub = self.mqtt_client.subscribe(f'{ns}{t2}')
        self.ad_award__ros_pub = self.create_publisher(KeyValue, t2, 10)

        # Bidder should publish its rejection notice
        # (publish to mqtt)
        print(f'Subscribing to ROS   | {t3}')
        print(f'Publishing to MQTT   | {ns}{t3}')
        self.ad_reject__ros_sub = self.create_subscription(String, t3, self.on_ROS_ad_reject, 10)
        self.ad_reject_mqtt_pub = lambda mqtt_client, msg : mqtt_client.publish(f'{ns}{t3}', msg)

    def attach_as_duckling(self):

        # Add additional callbacks to occur when the leader has approved the bid from the robot
        ns = 'ducklings'
        t = f'/{self.leader}/duckling/safe_node_list'

        # Connect to approved leader to recieve the list of safe nodes
        # (subscribe to mqtt)
        qos = QoSProfile(depth=1, durability=DurabilityPolicy.TRANSIENT_LOCAL)
        print(f'Subscribing to MQTT  | {ns}{t}')
        print(f'Publishing to ROS    | {t}')
        self.snl_mqtt_sub = self.mqtt_client.subscribe(f'{ns}{t}')
        self.snl__ros_pub = self.create_publisher(PoseArray, t, qos)

    def dettach_as_duckling(self):
        print('dettach_as_mother')
        pass

        # Unsubscribe from ROS
        # Unsubscribe from MQTT
        # Delete publishers to ROS
        # Delete publishers to MQTT
        # Delete reference to mother
        self.leader = None








    #########################
    ## Callbacks from MQTT ##
    #########################

    def on_mqtt_message(self, client, userdata, mqtt_msg):

        # Identify source of message
        t = mqtt_msg.topic
        msg = self.loads(mqtt_msg.payload)
        pprint(msg)
        
        # Possible topic ends
        t0 = '/duckling/advert'
        t1 = '/duckling/advert_bid'
        t2 = '/duckling/advert_award'
        t3 = '/duckling/advert_reject'
        t4 = '/duckling/safe_node_list'

        # Switch callback based on message

        #########################
        ## from mother...      ##
        #########################

        # If we receive an advert from any mother via mqtt, publish to ROS
        if t.endswith(t0):
            rosmsg = PoseStamped()
            rosmsg.pose.position.x = msg['pose']['position']['x']
            rosmsg.pose.position.y = msg['pose']['position']['y']
            rosmsg.pose.position.z = msg['pose']['position']['z']
            rosmsg.header.frame_id = msg['header']['frame_id']
            self.ad_ad__ros_pub.publish(rosmsg)
            return

        # If we receive an award from mother via mqtt, publish to ROS
        if t.endswith(t2):
            rosmsg = KeyValue()
            rosmsg.key = msg['key']
            rosmsg.value = msg['value']
            self.ad_award__ros_pub.publish(rosmsg)

            # If award was for this duckling, connect to additional topics
            if rosmsg.key == self.robot_name:
                self.attach_as_duckling()
            return

        # If we receive a safe node list via mqtt, publish to ROS
        if t.endswith(t4):
            rosmsg = PoseArray()
            rosmsg.header.frame_id = msg['header']['frame_id']
            for p in msg['poses']:
                pose = Pose()
                pose.position.x = p['position']['x']
                pose.position.y = p['position']['y']
                pose.position.z = p['position']['z']
                pose.orientation.x = p['orientation']['x']
                pose.orientation.y = p['orientation']['y']
                pose.orientation.z = p['orientation']['z']
                pose.orientation.w = p['orientation']['w']
                rosmsg.poses.append(pose)
            self.snl__ros_pub.publish(rosmsg)
            return

        #########################
        ## from duckling...    ##
        #########################

        # If we receive a bid from duckling via mqtt, publish to ROS
        if t.endswith(t1):
            rosmsg = KeyValue()
            rosmsg.key = msg['key']
            rosmsg.value = msg['value']
            self.ad_bid__ros_pub.publish(rosmsg)
            return

        # If we receive a reject from duckling via mqtt, publish to ROS and disconnect snl
        if t.endswith(t3):
            rosmsg = String()
            rosmsg.data = msg['data']
            self.ad_reject__ros_pub.publish(rosmsg)
            self.disconnect_from_disconnect(rosmsg)
            return


    ########################
    ## Callbacks from ROS ##
    ########################

    #################
    ## from mother ##
    #################

    def on_ROS_ad_ad(self, msg):
        print('\n', 'MSG: ad_ad')
        print(msg, '\n')

        # Reset connections
        print('Resetting any outstanding connections.')
        #self.connection_reset()
        time.sleep(1)

        # If mother sends a new advert, publish to MQTT
        self.ad_ad_mqtt_pub(self.mqtt_client, self.dumps(rosdict(msg)))

    def on_ROS_ad_award(self, msg):
        print('\n', 'MSG: ad_award')
        print(msg, '\n')

        # If mother sends an acceptance, publish to MQTT
        self.ad_award_mqtt_pub(self.mqtt_client, self.dumps(rosdict(msg)))
        self.attach_as_mother()
        if msg.key != self.robot_name:
            self.dettach_as_duckling()

    def on_ROS_snl(self, msg):
        print('\n', 'MSG: ad_snl')
        print(msg, '\n')

        # If mother sends safe node list, publish to MQTT
        self.snl_mqtt_pub(self.mqtt_client, self.dumps(rosdict(msg)))

    ###################
    ## from duckling ##
    ###################

    def on_ROS_ad_interest(self, msg):
        print('\n', 'MSG: ad_interest')
        print(msg, '\n')

        # Reset connections
        print('Resetting any outstanding connections.')
        #self.connection_reset()
        time.sleep(1)

        # If duckling shows interest in advert, open communication to mother
        self.connect_as_duckling(msg)

    def on_ROS_ad_bid(self, msg):

        # Leave a delay for connections to establish
        time.sleep(1)

        print('\n', 'MSG: ad_bid')
        print(msg, '\n')
        # If duckling bids on advert, publish to mqtt
        self.ad_bid_mqtt_pub(self.mqtt_client, self.dumps(rosdict(msg)))

    def on_ROS_ad_reject(self, msg):
        print('\n', 'MSG: ad_reject')
        print(msg, '\n')

        # If duckling rejects mother, publish to MQTT and disconnect communication
        self.reject_mqtt_pub(self.mqtt_client, self.dumps(rosdict(msg)))
        self.disconnect_from_mother()




def main(args=None):
    rclpy.init(args=args)

    robot_name = os.getenv('ROBOT_NAME')
    if not robot_name: quit('Please ensure $ROBOT_NAME is set... quitting...')
    QU = Quacker(robot_name, act_as_mother=True, act_as_duckling=True)
    rclpy.spin(QU)

    QU.destroy_node()
    rclpy.shutdown()


def mother(args=None):
    rclpy.init(args=args)

    robot_name = os.getenv('ROBOT_NAME')
    if not robot_name: quit('Please ensure $ROBOT_NAME is set... quitting...')
    QU = Quacker(robot_name, act_as_mother=True, act_as_duckling=False)
    rclpy.spin(QU)

    QU.destroy_node()
    rclpy.shutdown()


def duckling(args=None):
    rclpy.init(args=args)

    robot_name = os.getenv('ROBOT_NAME')
    if not robot_name: quit('Please ensure $ROBOT_NAME is set... quitting...')
    QU = Quacker(robot_name, act_as_mother=False, act_as_duckling=True)
    rclpy.spin(QU)

    QU.destroy_node()
    rclpy.shutdown()

