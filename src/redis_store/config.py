# -*- coding: utf-8 -*-
import json

import rospy

from std_msgs.msg import String
from redis_store_msgs.srv import (
    GetParam,
    SetParam,
    SaveDeleteParam,
    ResetParams,
)


class ConfigBase(object):
    _GET_PARAM_SRV_NAME = 'config_manager/get_param'
    _SET_PARAM_SRV_NAME = 'config_manager/set_param'
    _SAVE_PARAM_SRV_NAME = 'config_manager/save_param'
    _DELETE_PARAM_SRV_NAME = 'config_manager/delete_param'
    _RESET_PARAMS_SRV_NAME = 'config_manager/reset_params'
    _UPDATE_TOPIC_NAME = 'config_manager/update'


class ConfigClient(ConfigBase):
    def __init__(self, subscribe=False):
        if subscribe:
            self._update_sub = rospy.Subscriber(
                self._UPDATE_TOPIC_NAME, String, self._update_received
            )
        else:
            self._update_sub = None
        self._get_param = rospy.ServiceProxy(self._GET_PARAM_SRV_NAME, GetParam)
        self._set_param = rospy.ServiceProxy(self._SET_PARAM_SRV_NAME, SetParam)
        self._save_param = rospy.ServiceProxy(
            self._SAVE_PARAM_SRV_NAME, SaveDeleteParam
        )
        self._delete_param = rospy.ServiceProxy(
            self._DELETE_PARAM_SRV_NAME, SaveDeleteParam
        )
        self._reset_params = rospy.ServiceProxy(
            self._RESET_PARAMS_SRV_NAME, ResetParams
        )

        self.on_update_received = []

    def get_param(self, name):
        response = self._get_param(name)
        decoded = None
        if response.success:
            decoded = json.loads(response.param_value)
        return decoded

    def set_param(self, name, value):
        response = self._set_param(name, json.dumps(value))
        return response.success

    def save_param(self, name):
        response = self._save_param(name)
        return response.success

    def delete_param(self, name):
        response = self._delete_param(name)
        return response.success

    def reset_params(self):
        response = self._reset_params()
        return response.success

    def stop(self):
        if self._update_sub:
            self._update_sub.unregister()

    def _update_received(self, msg):
        for cb in self.on_update_received:
            cb(msg.data)
