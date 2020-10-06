import json

import rospy

from redis_store_msgs.msg import ParamUpdate
from redis_store_msgs.srv import (
    GetParam,
    SetParam,
    SaveDeleteParam,
    ImportExportParam,
    ResetParams,
)


class ConfigBase:
    _GET_PARAM_SRV_NAME = 'config_manager/get_param'
    _SET_PARAM_SRV_NAME = 'config_manager/set_param'
    _SAVE_PARAM_SRV_NAME = 'config_manager/save_param'
    _DELETE_PARAM_SRV_NAME = 'config_manager/delete_param'
    _RESET_PARAMS_SRV_NAME = 'config_manager/reset_params'
    _IMPORT_PARAM_SRV_NAME = 'config_manager/import_param'
    _EXPORT_PARAM_SRV_NAME = 'config_manager/export_param'
    _UPDATE_TOPIC_NAME = 'config_manager/update'


class ConfigClient(ConfigBase):
    def __init__(self, subscribe=False):
        if subscribe:
            self._update_sub = rospy.Subscriber(
                self._UPDATE_TOPIC_NAME, ParamUpdate, self._update_received
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
        self._import_param = rospy.ServiceProxy(
            self._IMPORT_PARAM_SRV_NAME, ImportExportParam
        )
        self._export_param = rospy.ServiceProxy(
            self._EXPORT_PARAM_SRV_NAME, ImportExportParam
        )
        self._reset_params = rospy.ServiceProxy(
            self._RESET_PARAMS_SRV_NAME, ResetParams
        )

        self.on_update_received = []

    def wait_for_service(self, timeout=None):
        self._get_param.wait_for_service(timeout)

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

    def import_param(self, name, file_path):
        response = self._import_param(name, file_path)
        return response.success

    def export_param(self, name, file_path):
        response = self._export_param(name, file_path)
        return response.success

    def reset_params(self):
        response = self._reset_params()
        return response.success

    def stop(self):
        if self._update_sub:
            self._update_sub.unregister()

    def _update_received(self, msg):
        try:
            decoded_value = json.loads(msg.param_value)
        except ValueError:
            decoded_value = None
        for cb in self.on_update_received:
            cb(msg.param_name, decoded_value)
