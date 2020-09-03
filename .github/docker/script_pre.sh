#!/bin/bash -xe

ROS_DISTRO=noetic

# For redis_store_msgs package deps
curl -1sLf \
    'https://dl.cloudsmith.io/public/zultron/hal_ros_control/cfg/setup/bash.deb.sh' \
    | bash

# Bootstrap ROS installation
# http://wiki.ros.org/noetic/Installation/Source

echo "deb http://packages.ros.org/ros/ubuntu $(lsb_release -sc) main" \
    > /etc/apt/sources.list.d/ros-latest.list
apt-key adv --keyserver 'hkp://keyserver.ubuntu.com:80' \
    --recv-key C1CF6E31E6BADE8868B172B4F42ED6FBAB17C654
apt-get update

pip3 install -U \
    vcstool \
    rosdep \
    rosinstall-generator \

apt-get install -y \
    build-essential

# Custom rosdep keys
mkdir -p /etc/ros/rosdep/sources.list.d
cat > /etc/ros/rosdep/machinekit-rosdep.yaml <<EOF
redis_store_msgs:
  debian: [ros-$ROS_DISTRO-redis-store-msgs]
  ubuntu: [ros-$ROS_DISTRO-redis-store-msgs]
EOF
cat > /etc/ros/rosdep/sources.list.d/10-local.list <<EOF
yaml file:///etc/ros/rosdep/machinekit-rosdep.yaml
EOF

rosdep init
rosdep update

# https://gist.github.com/awesomebytes/196eab972a94dd8fcdd69adfe3bd1152
pip3 install -U \
     bloom
pip3 install -U \
    bloom
source /etc/os-release
mv files/package.xml .
bloom-generate rosdebian --os-name $ID --os-version $VERSION_CODENAME \
    --ros-distro ${ROS_DISTRO}
