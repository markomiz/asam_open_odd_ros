from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    odd_yaml = LaunchConfiguration('odd_yaml')
    taxonomy_yaml = LaunchConfiguration('taxonomy_yaml')
    mappings_yaml = LaunchConfiguration('mappings_yaml')
    publish_hz = LaunchConfiguration('publish_hz')

    return LaunchDescription([
        DeclareLaunchArgument('odd_yaml'),
        DeclareLaunchArgument('taxonomy_yaml'),
        DeclareLaunchArgument('mappings_yaml'),
        DeclareLaunchArgument('publish_hz', default_value='5.0'),

        Node(
            package='asam_open_odd_ros',
            executable='odd_monitor',
            name='odd_monitor',
            output='screen',
            parameters=[
                {'odd_yaml': odd_yaml},
                {'taxonomy_yaml': taxonomy_yaml},
                {'mappings_yaml': mappings_yaml},
                {'publish_hz': publish_hz},
            ],
            remappings=[
                # expose outputs
                ('~/cod', 'cod'),
                ('~/report', 'odd_report'),
            ],
        )
    ])
