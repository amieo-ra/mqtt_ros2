import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient
from rclpy.executors import MultiThreadedExecutor, SingleThreadedExecutor
#from topological_navigation_msgs.msg import ExecutePolicyModeFeedback, ExecutePolicyModeResult
# from topological_navigation_msgs.msg import ExecutePolicyModeFeedback
from topological_navigation_msgs.msg import ExecutePolicyModeGoal
from topological_navigation_msgs.action import GotoNode, ExecutePolicyMode
from std_msgs.msg import String  # Adjust according to the data types you expect
from actionlib_msgs.msg import GoalID, GoalStatusArray
from rclpy.qos import QoSProfile, DurabilityPolicy, ReliabilityPolicy, HistoryPolicy
 
 
 
class ActionMiddleman(Node):
    def __init__(self):
        super().__init__('action_middleman')
        print("running actionmiddleman")
        # Initialize the action client
 
        self.action_server_name = '/topological_navigation'        
        self.client = ActionClient(self, GotoNode, self.action_server_name)

        self.qos = QoSProfile(depth=1, 
        #reliability=ReliabilityPolicy.RELIABLE,
        reliability=ReliabilityPolicy.RELIABLE,
        history=HistoryPolicy.KEEP_LAST,
        durability=DurabilityPolicy.TRANSIENT_LOCAL)

 
        # Subscribe to the custom topics
        self.goal_subscriber = self.create_subscription(ExecutePolicyModeGoal, 'topological_navigation/execute_policy_mode/goal', self.goal_callback, qos_profile=self.qos)
        self.cancel_subscriber = self.create_subscription(GoalID, 'topological_navigation/execute_policy_mode/cancel', self.cancel_callback, qos_profile=self.qos)
        # Publishers for feedback and result
        # self.feedback_publisher = self.create_publisher(ExecutePolicyModeFeedback, 'topological_navigation/execute_policy_mode/feedback', 10)
        # self.result_publisher = self.create_publisher(ExecutePolicyModeGoal, 'topological_navigation/execute_policy_mode/result', 10) #(ExecutePolicyModeResult, 'topological_navigation/execute_policy_mode/result', 10)
        # self.status_publisher = self.create_publisher(GoalStatusArray, 'topological_navigation/execute_policy_mode/status', 10)
        
        self.executor_goto_client = SingleThreadedExecutor()
        
        self.current_goal = None
        self.goal_handle = None
        print("waiting for topics")
 
    def goal_callback(self, msg):

        print("got to callback for goal")
        print("the goal message is:", msg)
 
        if not self.client.server_is_ready():
            self.get_logger().info("Waiting for the action server  {}...".format(self.action_server_name))
            self.client.wait_for_server(timeout_sec=2)
        
        if not self.client.server_is_ready():
            self.get_logger().info("action server  {} not responding ... can not perform any action".format(self.action_server_name))
            return
        
        self.get_logger().info("Executing the action...")
 
        # self.get_logger().info(f'Received new goal: {msg.data}')
 
        goal = msg.route.edge_id[-1].split('_')[-1]
        print("<<<<goal is>>>>:", goal)
        # Parse the message according to your actual message structure
        goal_msg = GotoNode.Goal()
        goal_msg.target = goal
        self.send_current_goal = self.client.send_goal_async(goal_msg, feedback_callback=self.feedback_callback) #COMMENTED OUT FOR TESTING
 
        while rclpy.ok():
            try:
                rclpy.spin_once(self, executor=self.executor_goto_client)
                if self.send_current_goal.done():
                    self.goal_handle = self.send_current_goal.result()
                    break
 
            except Exception as e:
                self.get_logger().error("Error while sending the goal to GOTO node{} ".format(e))
 
        if not self.goal_handle.accepted:
            self.get_logger.error('GOTO action is rejected')
            return False
        
        self.get_logger().info('The goal is accepted')
        self.goal_get_result_future = self.goal_handle.get_result_async()
        self.get_logger().info("Waiting for {} action to complete".format(self.action_server_name))
        
        while rclpy.ok():
            try:
                rclpy.spin_once(self, timeout_sec=0.2)
                if(self.early_terminate_is_required):
                   self.get_logger().warning("Not going to wait till finishing ongoing task, early termination is required ")
                   return False
                if self.goal_get_result_future.done():
                    status = self.goal_get_result_future.result().status
                    self.action_status = status
                    self.get_logger().info("Executing the action response with status {}".format(self.get_status_msg(self.action_status)))
                    self.current_goal.add_done_callback(self.goal_response_callback) # CHECK THIS...
                    return True
            except Exception as e:
                self.get_logger().error("Error while executing go to node policy {} ".format(e))
                return False  
 
 
    def cancel_callback(self, msg):
        self.get_logger().info('Received cancel request')
        if self.current_goal:
            cancel_future = self.current_goal.cancel_goal_async()
            # self.current_goal.cancel_goal_async()
 
                
        self.get_logger().info("Waiting till terminating the current preemption")
        while rclpy.ok():
            try:
                rclpy.spin_once(self, executor=self.executor_goto_client)
                # rclpy.spin_until_future_complete(self, cancel_future, executor=self.executor_goto_client, timeout_sec=2.0)
                if cancel_future.done() and self.goal_get_result_future.done():
                    self.action_status = self.goal_get_result_future.result().status
                    self.get_logger().info("The goal cancel error code {} ".format(self.get_goal_cancle_error_msg(cancel_future.result().return_code)))
                    return True
            except Exception as e:
                # self.goal_handle = None
                self.get_logger().error("Edge Action Manager: error while canceling the previous action")
                return False
            
 
    def feedback_callback(self, feedback_msg):
        self.nav_client_feedback = feedback_msg.feedback
        self.get_logger().info("feedback: {} ".format(self.nav_client_feedback))
        feedback_array = ExecutePolicyModeFeedback()
        # feedback_array.data = feedback_msg.feedback.partial_sequence
        # self.feedback_publisher.publish(feedback_array)
 
    def goal_response_callback(self, future):
        result = future.result().result
        self.get_logger().info(f'Goal completed with result: {result.sequence}')
        result_array = ExecutePolicyModeGoal # was ExecutePolicyModeResult()
        result_array.data = result.sequence
        self.result_publisher.publish(result_array)
 
def main(args=None):
    rclpy.init(args=args)
    middleman_node = ActionMiddleman()
    rclpy.spin(middleman_node)
    rclpy.shutdown()
 
if __name__ == '__main__':
    main()
