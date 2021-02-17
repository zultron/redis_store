import os

import pytest
import threading
import queue
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


class TestConfigClientConcurrencyGetParamSharedClient:
    # Test get_param() concurrency with threads sharing a ConfigClient
    # object

    num_iterations = 500
    name = "get_param_shared"

    param_data = dict(
        foo=1,
        bar=2,
        baz="bleb",
        quux=dict(x=1, y="flux"),
    )

    @pytest.fixture
    def seed_param_data(self, client):
        # Seed params for test
        for key, val in self.param_data.items():
            assert client.set_param(key, val) is True

        return self.param_data

    def thread_client(self):
        # Share one client among all threads
        if not hasattr(self, '_thread_client'):
            self._thread_client = ConfigClient(subscribe=True)
        return self._thread_client

    def test_multithreaded_client_config(self, node, client, seed_param_data):
        # Run parallel queries from threads, one client shared across threads
        # - Threading apparatus
        self.results = queue.Queue(maxsize=len(self.param_data))
        threads = dict()

        # - Init and start threads
        print("\n{self.name}:  Setup\n")
        for key, val in self.param_data.items():
            threads[key] = threading.Thread(
                target=self.thread_func,
                args=(key,),
            )
        print("{self.name}:  Start\n")
        for key in self.param_data.keys():
            threads[key].start()

        # - Wait for thread finish
        print("{self.name}:  Wait\n")
        for key, val in self.param_data.items():
            threads[key].join()

        # - Read and verify results
        print(f"{self.name}:  Finished with {self.results.qsize()} results\n")
        assert self.results.qsize() == len(self.param_data)
        while not self.results.empty():
            self.verify_result(*self.results.get())

    def thread_func(self, key):
        # Read and verify parameter in a loop, then record results
        failures = 0
        exceptions = 0
        count = 0
        val = self.param_data[key]
        client = self.thread_client()
        print(f"\n{self.name}:  thread_func:  {key} = {val}")
        print(f"{self.name}:      client:  {client}")
        for test_iter in range(self.num_iterations):
            count += 1
            try:
                param_val = client.get_param(key)
            except Exception as e:
                print(
                    f"{self.name}:  thread_func {key} exception at {count}:"
                    f"  {str(e)}"
                )
                exceptions += 1
                continue
            if param_val != val:
                failures += 1
                print(
                    f"{self.name}:  thread_func {key} failure at {count}:  "
                    f"{param_val} != {val}"
                )
            else:
                print(f"{self.name}:  thread_func:  {key} success at {count}")
        self.results.put((key, val, count, failures, exceptions))
        print(
            f"{self.name}:  thread_func:  {key} {failures}+{exceptions}/{count}"
        )

    def verify_result(self, key, val, count, failures, exceptions):
        print(
            f"{self.name}:  Result {key}='{val}' "
            f"failures+exceptions/count:  {failures}+{exceptions}/{count}"
        )
        assert self.param_data[key] == val  # Sanity check
        assert count == self.num_iterations
        assert failures == 0
        assert exceptions == 0


class TestConfigClientConcurrencyGetParamUnsharedClient(
    TestConfigClientConcurrencyGetParamSharedClient
):
    # Test get_param() concurrency with threads using separate
    # ConfigClient objects

    name = "get_param_unshared"

    def thread_client(self):
        # Use new client for each thread
        return ConfigClient(subscribe=True)


class TestConfigClientConcurrencySetParam(
    TestConfigClientConcurrencyGetParamSharedClient
):
    # Test set_param() concurrency with threads sharing a ConfigClient
    # object
    #
    # The threads iterate, each reading a parameter, incrementing it
    # and writing it back; starting values are set to prevent overlap
    # so problems are more evident

    num_iterations = 500
    name = "set_param"

    param_data = dict(
        foo=1000,
        bar=2000,
        baz=3000,
        quux=4000,
    )

    def thread_func(self, key):
        # Read, increment and write parameter in a loop, then record
        # results
        failures = 0
        exceptions = 0
        start_val = self.param_data[key]
        client = self.thread_client()
        val = client.get_param(key)
        print(f"\n{self.name}:  {key} = {start_val} client:  {client}")
        if val != start_val:
            print(
                f"{self.name}:  {key} initial val failed {val} != {start_val}"
            )
            failures += 1
        for test_iter in range(self.num_iterations):
            try:
                last_val = client.get_param(key)
            except Exception as e:
                print(f"{self.name}:  {key} exception at {val}:  {str(e)}")
                exceptions += 1
            else:
                if last_val != val:
                    failures += 1
                    print(f"{self.name}:  {key} failure {last_val} != {val}")
            val += 1

            try:
                if client.set_param(key, val):
                    print(f"{self.name}:  set {key} = {val}")
                else:
                    print(f"{self.name}:  failure to set {key} = {val}")
                    failures += 1
            except Exception as e:
                print(
                    f"{self.name}:  exception setting {key} = {val}:  {str(e)}"
                )
                exceptions += 1

        self.results.put((key, val, failures, exceptions))
        print(f"{self.name}:  {key} {failures}+{exceptions}/{val}")

    def verify_result(self, key, val, failures, exceptions):
        start_val = self.param_data[key]
        iters = val - start_val
        print(
            f"{self.name}:  Fail/except {key}:  {failures}+{exceptions}/{iters}"
        )
        assert iters == self.num_iterations
        assert failures == 0
        assert exceptions == 0
