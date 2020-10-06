import os

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
        namespace=redis_namespace, host=redis_host, port=redis_port, db=redis_db
    )


@pytest.fixture
def data_file(tmpdir):
    f = tmpdir.join('stiffen.json')
    f.write(
        '''\
{"sharpen": "miss"}
'''
    )
    return str(f)


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


@pytest.mark.dependency()
def test_set_param_service_overwrites_sub_keys_in_ros_param(node, client):
    rospy.set_param('angry/over', "47j")

    success = client.set_param('angry', {"over": "Lns3Br3"})

    assert success is True
    assert rospy.get_param('angry') == {"over": "Lns3Br3"}
    assert rospy.get_param('angry/over') == "Lns3Br3"


@pytest.mark.dependency()
def test_set_param_service_cleans_up_sub_keys_in_redis_db(node, client, redis):
    redis['angry/over'] = "47j"

    success = client.set_param('angry', {"over": "Lns3Br3"})

    assert success is True
    assert 'angry' not in redis
    assert redis['angry/over'] == "Lns3Br3"


@pytest.mark.dependency()
def test_import_param_service_updates_ros_parameter(node, client, data_file):
    success = client.import_param('hurt', data_file)

    assert success is True
    assert rospy.get_param('hurt') == dict(sharpen="miss")


@pytest.mark.dependency()
def test_import_param_service_update_redis_db(node, client, redis, data_file):
    success = client.import_param('narrow', data_file)

    assert success is True
    assert redis['narrow/sharpen'] == "miss"


@pytest.mark.dependency()
def test_import_param_service_cleans_up_sub_keys_in_ros_param(
    node, client, data_file
):
    rospy.set_param('someone', dict(sharpen="miss"))
    rospy.set_param('someone/strip', 365)

    success = client.import_param('someone', data_file)

    assert success is True
    assert rospy.get_param('someone', None) == dict(sharpen="miss")
    assert rospy.get_param('someone/sharpen', None) == "miss"
    assert rospy.get_param('someone/strip', None) is None


@pytest.mark.dependency()
def test_import_param_service_cleans_up_sub_keys_in_redis_db(
    node, client, redis, data_file
):
    redis['someone/sharpen'] = "applaud"
    redis['someone/strip'] = 365

    success = client.import_param('someone', data_file)

    assert success is True
    assert 'someone' not in redis
    assert 'someone/strip' not in redis
    assert redis['someone/sharpen'] == "miss"


def test_export_param_service_exports_data_to_file(node, client, data_file):
    rospy.set_param('tray', ["degree", "favorite", "rapid", "pad", "rake"])

    success = client.export_param('tray', data_file)

    assert success is True
    assert os.path.exists(data_file)
    with open(data_file) as f:
        data = f.read()
    assert data == '["degree", "favorite", "rapid", "pad", "rake"]'


@pytest.mark.dependency(
    depends=[
        'test_set_param_service_updates_ros_parameter',
        'test_set_param_service_updates_redis_db',
    ]
)
def test_param_name_is_normalized_correctly(node, client, redis):
    client.set_param('/reduce', 16)
    client.set_param('fierce/expense/', "mad")

    assert redis['reduce'] == 16
    assert client.get_param('reduce') == 16
    assert client.get_param('/reduce') == 16
    assert redis['fierce/expense'] == "mad"
    assert client.get_param('fierce/expense') == "mad"
    assert client.get_param('fierce/expense/') == "mad"


def test_save_param_stores_ros_param_into_redis_db(node, client, redis):
    rospy.set_param('wrabbe', 908)

    success = client.save_param('wrabbe')

    assert success is True
    assert redis['wrabbe'] == 908


def test_save_param_stores_sub_keys_into_redis_db(node, client, redis):
    rospy.set_param('insult', {"snow": "1xN", "night": 246})

    success = client.save_param('insult')

    assert success is True
    assert 'insult' not in redis
    assert redis['insult/snow'] == "1xN"
    assert redis['insult/night'] == 246


def test_save_param_fails_when_ros_param_does_not_exist(node, client, redis):
    success = client.save_param('bullpout')

    assert success is False


@pytest.mark.dependency(
    depends=[
        'test_set_param_service_updates_ros_parameter',
        'test_set_param_service_updates_redis_db',
    ]
)
def test_delete_param_removes_ros_param_and_redis_entry(node, client, redis):
    client.set_param('severe', 'MMWMJWL')

    success = client.delete_param('severe')

    assert success is True
    assert rospy.get_param('severe', None) is None
    assert redis.get('severe', None) is None


@pytest.mark.dependency(
    depends=[
        'test_set_param_service_overwrites_sub_keys_in_ros_param',
        'test_set_param_service_cleans_up_sub_keys_in_redis_db',
    ]
)
def test_delete_param_removes_subkey_from_ros_param_and_redis(
    node, client, redis
):
    client.set_param('bread', {"spit": "5qvgNvf", "sick": 335})

    success = client.delete_param('bread')

    assert success is True
    assert rospy.get_param('bread', None) is None
    assert rospy.get_param('bread/spit', None) is None
    assert rospy.get_param('bread/sick', None) is None
    assert redis.get('bread', None) is None
    assert redis.get('bread/spit', None) is None
    assert redis.get('bread/sick', None) is None


@pytest.mark.dependency(
    depends=[
        'test_verify_that_example_default_parameters_have_been_set',
        'test_set_param_service_updates_ros_parameter',
        'test_set_param_service_updates_redis_db',
    ]
)
def test_reset_params_resets_params_back_to_default(node, client, redis):
    client.set_param('foo/bar', 90)

    success = client.reset_params()

    assert success is True
    assert rospy.get_param('foo/bar') == 'something'
    assert redis.get('foo/bar', None) is None


@pytest.mark.dependency(
    depends=[
        'test_verify_that_example_default_parameters_have_been_set',
        'test_set_param_service_updates_ros_parameter',
        'test_set_param_service_updates_redis_db',
    ]
)
def test_reset_params_removes_additional_params_from_redis(node, client, redis):
    client.set_param('octonare', 530)

    success = client.reset_params()

    assert success is True
    assert rospy.get_param('octonare', None) is None
    assert redis.get('octonare', None) is None
