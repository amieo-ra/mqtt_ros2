from setuptools import setup
import os
from glob import glob

package_name = 'mqtt_ros2'

setup(
    name=package_name,
    version='3.0.4',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        (os.path.join('share', package_name, 'launch'), glob(os.path.join('launch', '*launch.[pxy][yma]*')))
    ],
    install_requires=['sympy>=1.5.1'],
    zip_safe=True,
    maintainer='Amie Owen',
    maintainer_email='amieo@live.co.uk',
    description='The ros2 version of the mqtt_pseudo_bridge to run with the Coordinator',
    license='MIT',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [
            'mqtt_pseudo_bridge.py = mqtt_ros2.mqtt_pseudo_bridge:main',
            'send_details_to_coordinator.py = mqtt_ros2.send_details_to_coordinator:main',
            'middle_node.py = mqtt_ros2.middle_node:main'
        ],
    },

)