# -*- coding: utf-8 -*-
import pytest
import rospy

from redis_store import RedisDict, ConfigClient


@pytest.fixture
def node():
    return rospy.init_node('pytest', anonymous=True)


@pytest.fixture
def client():
    return ConfigClient(subscribe=True)


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


def test_get_param_returns_parameter_from_defaults(node, client):
    param = client.get_param('foo/bar')

    assert param == 'something'


def test_get_param_with_nonexisting_parameter_returns_none(node, client):
    param = client.get_param('gemmy/blader')

    assert param is None


def test_ros_param_that_is_not_in_defaults_is_read_back_correctly(node, client):
    rospy.set_param('ladakhi', '7Cq')

    param = client.get_param('ladakhi')

    assert param == '7Cq'


def test_ros_param_with_complex_data_type_is_read_back_as_json(node, client):
    rospy.set_param('lucarnes', [989, 292, 597, 584, 500])

    param = client.get_param('lucarnes')

    assert param == [989, 292, 597, 584, 500]


@pytest.mark.dependency()
def test_set_param_service_updates_ros_parameter(node, client):
    success = client.set_param('prees', 514.29)

    assert success is True
    assert rospy.get_param('prees') == pytest.approx(514.29)


@pytest.mark.dependency()
def test_set_param_service_updates_redis_db(node, client, redis):
    success = client.set_param('euclases', 'LR1WJO')

    assert success is True
    assert redis['euclases'] == 'LR1WJO'


def test_save_param_stores_ros_param_into_redis_db(node, client, redis):
    rospy.set_param('wrabbe', 908)

    success = client.save_param('wrabbe')

    assert success is True
    assert redis['wrabbe'] == 908


def test_save_param_fails_when_ros_param_does_not_exist(node, client, redis):
    success = client.save_param('bullpout')

    assert success is False


@pytest.mark.dependency(depends=[
    'test_set_param_service_updates_ros_parameter',
    'test_set_param_service_updates_redis_db',
])
def test_delete_param_removes_ros_param_and_redis_entry(node, client, redis):
    client.set_param('severe', 'MMWMJWL')

    success = client.delete_param('severe')

    assert success is True
    assert rospy.get_param('severe', None) is None
    assert redis.get('severe', None) is None


@pytest.mark.dependency(depends=[
    'test_verify_that_example_default_parameters_have_been_set',
    'test_set_param_service_updates_ros_parameter',
    'test_set_param_service_updates_redis_db',
])
def test_reset_params_resets_params_back_to_default(node, client, redis):
    client.set_param('foo/bar', 90)

    success = client.reset_params()

    assert success is True
    assert rospy.get_param('/foo/bar') == 'something'
    assert redis.get('/foo/bar', None) is None


@pytest.mark.dependency(depends=[
    'test_verify_that_example_default_parameters_have_been_set',
    'test_set_param_service_updates_ros_parameter',
    'test_set_param_service_updates_redis_db',
])
def test_reset_params_removes_additional_params_from_redis(node, client, redis):
    client.set_param('octonare', 530)

    success = client.reset_params()

    assert success is True
    assert rospy.get_param('octonare', None) is None
    assert redis.get('octonare', None) is None
