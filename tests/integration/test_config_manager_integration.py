# -*- coding: utf-8 -*-
import json

import pytest
import rospy

from redis_store.srv import GetParam, SetParam, SaveDeleteParam, ResetParams
from redis_store import RedisDict


@pytest.fixture
def node():
    return rospy.init_node('pytest', anonymous=True)


@pytest.fixture
def get_param():
    return rospy.ServiceProxy('config_manager/get_param', GetParam)


@pytest.fixture
def set_param():
    return rospy.ServiceProxy('config_manager/set_param', SetParam)


@pytest.fixture
def save_param():
    return rospy.ServiceProxy('config_manager/save_param', SaveDeleteParam)


@pytest.fixture
def delete_param():
    return rospy.ServiceProxy('config_manager/delete_param', SaveDeleteParam)


@pytest.fixture
def reset_params():
    return rospy.ServiceProxy('config_manager/reset_params', ResetParams)


@pytest.fixture
def redis():
    redis_host = rospy.get_param('redis_host')
    redis_port = rospy.get_param('redis_port')
    redis_namespace = rospy.get_param('redis_namespace')
    redis_db = rospy.get_param('redis_db')
    return RedisDict(
        namespace=redis_namespace,
        host=redis_host,
        port=redis_port,
        db=redis_db,
    )


@pytest.mark.dependency()
def test_verify_that_example_default_parameters_have_been_set(node, redis):
    assert rospy.get_param('global_waypoints/foo') == [0, 1, 2, 3, 4, 5]
    assert rospy.get_param('foo/bar') == 'something'


def test_get_param_service_returns_parameter_from_defaults(node, get_param):
    res = get_param('foo/bar')

    assert res.success is True
    assert res.param_value == '"something"'


def test_get_param_with_nonexisting_parameter_is_unsuccessful(node, get_param):
    res = get_param('gemmy/blader')

    assert res.success is False


def test_ros_param_that_is_not_in_defaults_is_read_back_correctly(node, get_param):
    rospy.set_param('ladakhi', '7Cq')

    res = get_param('ladakhi')

    assert res.success is True
    assert res.param_value == '"7Cq"'


def test_ros_param_with_complex_data_type_is_read_back_as_json(node, get_param):
    rospy.set_param('lucarnes', [989, 292, 597, 584, 500])

    res = get_param('lucarnes')

    assert res.success is True
    assert res.param_value == '[989, 292, 597, 584, 500]'


@pytest.mark.dependency()
def test_set_param_service_updates_ros_parameter(node, set_param):
    res = set_param('prees', json.dumps(514.29))

    assert res.success is True
    assert rospy.get_param('prees') == pytest.approx(514.29)


@pytest.mark.dependency()
def test_set_param_service_updates_redis_db(node, set_param, redis):
    res = set_param('euclases', json.dumps('LR1WJO'))

    assert res.success is True
    assert redis['euclases'] == 'LR1WJO'


def test_save_param_stores_ros_param_into_redis_db(node, save_param, redis):
    rospy.set_param('wrabbe', 908)

    res = save_param('wrabbe')

    assert res.success is True
    assert redis['wrabbe'] == 908


def test_save_param_fails_when_ros_param_does_not_exist(node, save_param, redis):
    res = save_param('bullpout')

    assert res.success is False


@pytest.mark.dependency(depends=[
    'test_set_param_service_updates_ros_parameter',
    'test_set_param_service_updates_redis_db',
])
def test_delete_param_removes_ros_param_and_redis_entry(node, delete_param, set_param, redis):
    set_param('severe', json.dumps('MMWMJWL'))

    res = delete_param('severe')

    assert res.success is True
    assert rospy.get_param('severe', None) is None
    assert redis.get('severe', None) is None


@pytest.mark.dependency(depends=[
    'test_verify_that_example_default_parameters_have_been_set',
    'test_set_param_service_updates_ros_parameter',
    'test_set_param_service_updates_redis_db',
])
def test_reset_params_resets_params_back_to_default(node, set_param, reset_params, redis):
    set_param('foo/bar', json.dumps(90))

    res = reset_params()

    assert res.success is True
    assert rospy.get_param('/foo/bar') == 'something'
    assert redis.get('/foo/bar', None) is None


@pytest.mark.dependency(depends=[
    'test_verify_that_example_default_parameters_have_been_set',
    'test_set_param_service_updates_ros_parameter',
    'test_set_param_service_updates_redis_db',
])
def test_reset_params_removes_additional_params_from_redis(node, set_param, reset_params, redis):
    set_param('octonare', json.dumps(530))

    res = reset_params()

    assert res.success is True
    assert rospy.get_param('octonare', None) is None
    assert redis.get('octonare', None) is None
