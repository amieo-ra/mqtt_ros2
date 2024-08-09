import rclpy
from rclpy.node import Node
from rclpy.action import ActionClient
from rclpy.executors import MultiThreadedExecutor, SingleThreadedExecutor
#from topological_navigation_msgs.msg import ExecutePolicyModeFeedback, ExecutePolicyModeResult
# from topological_navigation_msgs.msg import ExecutePolicyModeFeedback
from topological_navigation_msgs.msg import ExecutePolicyModeGoal, ExecutePolicyModeFeedback
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
        #self.cancel_subscriber = self.create_subscription(GoalID, 'topological_navigation/execute_policy_mode/cancel', self.cancel_callback, qos_profile=self.qos)
        # Publishers for feedback and result
        # self.feedback_publisher = self.create_publisher(ExecutePolicyModeFeedback, 'topological_navigation/execute_policy_mode/feedback', 10)
        # self.result_publisher = self.create_publisher(ExecutePolicyModeGoal, 'topological_navigation/execute_policy_mode/result', 10) #(ExecutePolicyModeResult, 'topological_navigation/execute_policy_mode/result', 10)
        self.status_publisher = self.create_publisher(GoalStatusArray, 'topological_navigation/execute_policy_mode/status', 10)
        
        self.executor_goto_client = SingleThreadedExecutor()
        
        self.target_goal = None
        self.goal_handle = None
        navgoal = GotoNode.Goal()
        self.goal = navgoal
        #navgoal.target = feedback.marker_name
        print("waiting for topics")

        self.goal_cancel_error_codes = {} 
        self.goal_cancel_error_codes[0] = "ERROR_NONE"
        self.goal_cancel_error_codes[1] = "ERROR_REJECTED"
        self.goal_cancel_error_codes[2] = "ERROR_UNKNOWN_GOAL_ID"
        self.goal_cancel_error_codes[3] = "ERROR_GOAL_TERMINATED"

    def feedback_callback(self, feedback_msg):
        print("got to feedback")
        self.nav_client_feedback = feedback_msg.feedback
        self.get_logger().info("feedback: {} ".format(self.nav_client_feedback))
        return 
 
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
 
        self.future_goal = msg.route.edge_id[-1].split('_')[-1]
        print("<<<<goal is>>>>:", self.future_goal)
        # Parse the message according to your actual message structure
        #self.target_goal = GotoNode.Goal()
        #self.target_goal.target = goal

        self.execute_go_to_goal()
    

    def send_goal_request(self, send_goal_future, msg):

        while rclpy.ok():
            print("4444")
            try:
                print("5555")
                # rclpy.spin_once(self)
                # print("send_goal_future ", send_goal_future)
                rclpy.spin_once(self, executor=self.executor_goto_client, timeout_sec=0.5)
                print("6666")
                print("callback is:", send_goal_future.result())
                # rclpy.spin_until_future_complete(self, send_goal_future, executor=self.executor_nav_client, timeout_sec=2.0)
                if send_goal_future.done():
                    print("7777")
                    self.goal_handle = send_goal_future.result()
                    print("8888")
                    break
                else:
                    print("9999")
                    break
            except Exception as e:
                self.get_logger().error("Edge Action Manager: Nav2 server got some unexpected errors : {} while executing  send_goal_request {}".format(e, msg))
                return False 
                  
        if not self.goal_handle.accepted:
            self.get_logger().error('Edge Action Manager: The goal rejected: {}'.format(msg))
            return False
        self.get_logger().info('Edge Action Manager: The goal accepted for {}'.format(msg))
        return True 
     
    
    def processing_goal_request(self, target_action):
        if self.goal_handle is None:
            self.get_logger().error("Edge Action Manager: Something wrong with the goal request, there is no goal to process {}".format(self.action_status))
            return True
        self.goal_get_result_future = self.goal_handle.get_result_async()
        self.get_logger().info("Edge Action Manager: Waiting for {} action to complete".format(self.action_server_name))
        while rclpy.ok():
            try:
                # rclpy.spin_once(self)
                # print("goal_get_result_future ", self.goal_get_result_future)
                rclpy.spin_once(self, executor=self.executor_goto_client, timeout_sec=1.5)
                # rclpy.spin_until_future_complete(self, self.goal_get_result_future, executor=self.executor_nav_client, timeout_sec=2.0)
                if self.goal_get_result_future.done():
                    status = self.goal_get_result_future.result().status
                    self.action_status = status
                    self.get_logger().info("Go to node: Executing the action response with status")
                    #self.current_action = self.action_name
                    self.goal_resposne = self.goal_get_result_future.result()
            except Exception as e:
                # self.goal_handle = Nosne 
                self.get_logger().error("Edge Action Manager: Nav2 server got some unexpected errors: {} while executing processing_goal_request {}".format(e, target_action))
                return False
        return True 
    

    def execute_go_to_goal(self,):
        print("in executing function")
        #target_goal = GotoNode.Goal()
        #target_goal.target = self.goal ####got up to here
        self.goal.target = self.future_goal
        print("1111")
        send_goal_future = self.client.send_goal_async(self.goal, feedback_callback=self.feedback_callback)
        print("2222")
        print("result is:", send_goal_future.result())
        #goal_accepted = self.send_goal_request(send_goal_future, "requesting go to goal")
        #print("3333")
        #if(goal_accepted == False):
        #    return False 
        #processed_goal = self.processing_goal_request("actioning go to goal")
        #return processed_goal
      
 
    def cancel_callback(self, msg):
        self.get_logger().info('Received cancel request')
        if self.target_goal:
            cancel_future = self.current_goal.cancel_goal_async()
            # self.current_goal.cancel_goal_async()
 
                
        self.get_logger().info("Waiting till terminating the current preemption")
        while rclpy.ok():
            try:
                rclpy.spin_once(self, executor=self.executor_goto_client)
                # rclpy.spin_until_future_complete(self, cancel_future, executor=self.executor_goto_client, timeout_sec=2.0)
                if cancel_future.done() and self.goal_get_result_future.done():
                    self.action_status = self.goal_get_result_future.result().status
                    self.get_logger().info("The goal cancel error code {} ".format(self.get_goal_cancel_error_msg(cancel_future.result().return_code)))
                    return True
            except Exception as e:
                # self.goal_handle = None
                self.get_logger().error("Edge Action Manager: error while canceling the previous action")
                return False
            
    def get_goal_cancel_error_msg(self, status_code):
        try:
            return self.goal_cancel_error_codes[status_code]
        except Exception as e:
            self.get_logger().error("Goal cancel code {}".format(status_code))
            return self.goal_cancel_error_codes[0]
            
 
    def feedback_callback(self, feedback_msg):
        self.nav_client_feedback = feedback_msg.feedback
        self.get_logger().info("feedback: {} ".format(self.nav_client_feedback))
        feedback_array = ExecutePolicyModeFeedback()
        # feedback_array.data = feedback_msg.feedback.partial_sequence
        # self.feedback_publisher.publish(feedback_array)
        return
 
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
