# -*- coding: utf-8 -*-
import json
from collections import namedtuple

import pytest
import redis
from redis_store.redis_dict import RedisDict

# !! Make sure you don't have keys named like this, they will be deleted.
TEST_NAMESPACE_PREFIX = 'test_rd'

redis_config = {'host': 'localhost', 'port': 6379, 'db': 0}


def encode_data(data):
    return json.dumps(data)


def decode_data(data):
    return json.loads(data)


def create_redis_dict(namespace=TEST_NAMESPACE_PREFIX, **kwargs):
    config = redis_config.copy()
    config.update(kwargs)
    return RedisDict(namespace=namespace, **config)


def clear_test_namespace(redisdb):
    for key in redisdb.scan_iter('{}:*'.format(TEST_NAMESPACE_PREFIX)):
        redisdb.delete(key)


@pytest.fixture()
def db():
    Db = namedtuple('Db', 'r redisdb')
    redisdb = redis.StrictRedis(**redis_config)
    clear_test_namespace(redisdb)
    r = create_redis_dict()
    yield Db(r=r, redisdb=redisdb)
    clear_test_namespace(redisdb)


def test_keys_empty(db):
    """Calling RedisDict.keys() should return an empty list."""
    keys = db.r.keys()

    assert keys == []


def test_set_namespace(db):
    """Test that RedisDict keys are inserted with the given namespace."""
    db.r['foo'] = 'bar'

    expected_keys = ['{}:foo'.format(TEST_NAMESPACE_PREFIX)]
    actual_keys = db.redisdb.keys('{}:*'.format(TEST_NAMESPACE_PREFIX))

    assert expected_keys == actual_keys


def test_set_and_get(db):
    """Test setting a key and retrieving it."""
    db.r['foobar'] = 'barbar'

    assert db.r['foobar'] == 'barbar'


def test_set_none_and_get_none(db):
    """Test setting a key with no value and retrieving it."""
    db.r['foobar'] = None

    assert db.r['foobar'] is None


def test_set_and_get_multiple(db):
    """Test setting two different keys with two different values, and reading them."""
    db.r['foobar1'] = 'barbar1'
    db.r['foobar2'] = 'barbar2'

    assert db.r['foobar1'] == 'barbar1'
    assert db.r['foobar2'] == 'barbar2'


@pytest.mark.parametrize(
    'data',
    [
        {'wonner': 888.66, 'deplored': 'FaX5yjxS'},
        [756.54, 52.57, 395.22, 278.86, 40.59],
    ],
)
def test_setting_and_getting_complex_python_data_types_works(db, data):
    db.r['ssp'] = data

    assert db.r['ssp'] == data


def test_get_nonexisting(db):
    """Test that retrieving a non-existing key raises a KeyError."""
    with pytest.raises(KeyError):
        _ = db.r['nonexistingkey']  # noqa: F841


def test_get_with_existing_key_returns_existing_key(db):
    db.r['barrage'] = 989

    assert db.r.get('barrage') == 989


def test_get_with_nonexisting_key_returns_none(db):
    assert db.r.get('expel') is None


def test_get_with_nonexistent_key_and_default_returns_default(db):
    assert db.r.get('cathect', 504) == 504


def test_delete(db):
    """Test deleting a key."""
    db.r['foobargone'] = 'bars'

    del db.r['foobargone']

    assert db.redisdb.get('foobargone') is None


def test_contains_empty(db):
    """Tests the __contains__ function with no keys set."""
    assert 'foobar' not in db.r


def test_contains_nonempty(db):
    """Tests the __contains__ function with keys set."""
    db.r['foobar'] = 'barbar'

    assert 'foobar' in db.r


def test_repr_empty(db):
    """Tests the __repr__ function with no keys set."""
    expected_repr = str({})
    actual_repr = repr(db.r)

    assert actual_repr == expected_repr


def test_repr_nonempty(db):
    """Tests the __repr__ function with keys set."""
    db.r['foobars'] = 'barrbars'
    expected_repr = str({u'foobars': encode_data('barrbars').decode()})
    actual_repr = repr(db.r)

    assert actual_repr == expected_repr


def test_str_nonempty(db):
    """Tests the __repr__ function with keys set."""
    db.r['foobars'] = 'barrbars'
    expected_str = str({u'foobars': encode_data('barrbars').decode()})
    actual_str = str(db.r)

    assert actual_str == expected_str


def test_len_empty(db):
    """Tests the __repr__ function with no keys set."""
    assert len(db.r) == 0


def test_len_nonempty(db):
    """Tests the __repr__ function with keys set."""
    db.r['foobar1'] = 'barbar1'
    db.r['foobar2'] = 'barbar2'

    assert len(db.r) == 2


def test_to_dict_empty(db):
    """Tests the to_dict function with no keys set."""
    expected_dict = {}
    actual_dict = db.r.to_dict()

    assert actual_dict == expected_dict


def test_to_dict_nonempty(db):
    """Tests the to_dict function with keys set."""
    db.r['foobar'] = 'barbaros'
    expected_dict = {'foobar': encode_data('barbaros')}
    actual_dict = db.r.to_dict()

    assert actual_dict == expected_dict


def test_chain_set_1(db):
    """Test setting a chain with 1 element."""
    db.r.chain_set(['foo'], 'melons')

    expected_key = '{}:foo'.format(TEST_NAMESPACE_PREFIX)

    assert db.redisdb.get(expected_key) == encode_data('melons')


def test_chain_set_2(db):
    """Test setting a chain with 2 elements."""
    db.r.chain_set(['foo', 'bar'], 'melons')

    expected_key = '{}:foo:bar'.format(TEST_NAMESPACE_PREFIX)

    assert db.redisdb.get(expected_key) == encode_data('melons')


def test_chain_set_overwrite(db):
    """Test setting a chain with 1 element and then overwriting it."""
    db.r.chain_set(['foo'], 'melons')
    db.r.chain_set(['foo'], 'bananas')

    expected_key = '{}:foo'.format(TEST_NAMESPACE_PREFIX)

    assert db.redisdb.get(expected_key) == encode_data('bananas')


def test_chain_get_1(db):
    """Test setting and getting a chain with 1 element."""
    db.r.chain_set(['foo'], 'melons')

    assert db.r.chain_get(['foo']) == 'melons'


def test_chain_get_empty(db):
    """Test getting a chain that has not been set."""
    with pytest.raises(KeyError):
        _ = db.r.chain_get(['foo'])  # noqa: F841


def test_chain_get_2(db):
    """Test setting and getting a chain with 2 elements."""
    db.r.chain_set(['foo', 'bar'], 'melons')

    assert db.r.chain_get(['foo', 'bar']) == 'melons'


def test_chain_del_1(db):
    """Test setting and deleting a chain with 1 element."""
    db.r.chain_set(['foo'], 'melons')
    db.r.chain_del(['foo'])

    with pytest.raises(KeyError):
        _ = db.r.chain_get(['foo'])  # noqa: F841


def test_chain_del_2(db):
    """Test setting and deleting a chain with 2 elements."""
    db.r.chain_set(['foo', 'bar'], 'melons')
    db.r.chain_del(['foo', 'bar'])

    with pytest.raises(KeyError):
        _ = db.r.chain_get(['foo', 'bar'])  # noqa: F841


def test_expire_context(db):
    """Test adding keys with an expire value by using the contextmanager."""
    with db.r.expire_at(3600):
        db.r['foobar'] = 'barbar'

    actual_ttl = db.redisdb.ttl('{}:foobar'.format(TEST_NAMESPACE_PREFIX))

    assert actual_ttl == pytest.approx(3600, rel=2)


def test_expire_keyword(db):
    """Test ading keys with an expire value by using the expire config keyword."""
    r = create_redis_dict(expire=3600)

    r['foobar'] = 'barbar'
    actual_ttl = db.redisdb.ttl('{}:foobar'.format(TEST_NAMESPACE_PREFIX))

    assert actual_ttl == pytest.approx(3600, rel=2)


def test_iter(db):
    """Tests the __iter__ function."""
    key_values = {'foobar1': 'barbar1', 'foobar2': 'barbar2'}

    for key, value in key_values.items():
        db.r[key] = value

    # TODO made the assumption that iterating the redisdict should return keys, like a normal dict
    for key in db.r:
        assert db.r[key], key_values[key]


def test_multi_get_with_key_none(db):
    """Tests that multi_get with key None raises TypeError."""
    with pytest.raises(TypeError):
        db.r.multi_get(None)


def test_multi_get_empty(db):
    """Tests the multi_get function with no keys set."""
    assert db.r.multi_get('foo') == []


def test_multi_get_nonempty(db):
    """Tests the multi_get function with 3 keys set, get 2 of them."""
    db.r['foobar'] = 'barbar'
    db.r['foobaz'] = 'bazbaz'
    db.r['goobar'] = 'borbor'

    expected_result = ['barbar', 'bazbaz']

    assert set(db.r.multi_get('foo')) == set(expected_result)


def test_multi_get_chain_with_key_none(db):
    """Tests that multi_chain_get with key None raises TypeError."""
    with pytest.raises(TypeError):
        db.r.multi_chain_get(None)


def test_multi_chain_get_empty(db):
    """Tests the multi_chain_get function with no keys set."""
    assert db.r.multi_chain_get(['foo']) == []


def test_multi_chain_get_nonempty(db):
    """Tests the multi_chain_get function with keys set."""
    db.r.chain_set(['foo', 'bar', 'bar'], 'barbar')
    db.r.chain_set(['foo', 'bar', 'baz'], 'bazbaz')
    db.r.chain_set(['foo', 'baz'], 'borbor')

    # redis.mget seems to sort keys in reverse order here
    expected_result = [u'bazbaz', u'barbar']

    assert set(db.r.multi_chain_get(['foo', 'bar'])) == set(expected_result)


def test_multi_dict_empty(db):
    """Tests the multi_dict function with no keys set."""
    assert db.r.multi_dict('foo') == {}


def test_multi_dict_one_key(db):
    """Tests the multi_dict function with 1 key set."""
    db.r['foobar'] = 'barbar'
    expected_dict = {u'foobar': u'barbar'}
    assert db.r.multi_dict('foo') == expected_dict


def test_multi_dict_two_keys(db):
    """Tests the multi_dict function with 2 keys set."""
    db.r['foobar'] = 'barbar'
    db.r['foobaz'] = 'bazbaz'
    expected_dict = {u'foobar': u'barbar', u'foobaz': u'bazbaz'}
    assert db.r.multi_dict('foo') == expected_dict


def test_multi_dict_complex(db):
    """Tests the multi_dict function by setting 3 keys and matching 2."""
    db.r['foobar'] = 'barbar'
    db.r['foobaz'] = 'bazbaz'
    db.r['goobar'] = 'borbor'
    expected_dict = {u'foobar': u'barbar', u'foobaz': u'bazbaz'}
    assert db.r.multi_dict('foo') == expected_dict


def test_multi_del_empty(db):
    """Tests the multi_del function with no keys set."""
    assert db.r.multi_del('foobar') == 0


def test_multi_del_one_key(db):
    """Tests the multi_del function with 1 key set."""
    db.r['foobar'] = 'barbar'
    assert db.r.multi_del('foobar') == 1
    assert db.redisdb.get('foobar') is None


def test_multi_del_two_keys(db):
    """Tests the multi_del function with 2 keys set."""
    db.r['foobar'] = 'barbar'
    db.r['foobaz'] = 'bazbaz'
    assert db.r.multi_del('foo') == 2
    assert db.redisdb.get('foobar') is None
    assert db.redisdb.get('foobaz') is None


def test_multi_del_complex(db):
    """Tests the multi_del function by setting 3 keys and deleting 2."""
    db.r['foobar'] = 'barbar'
    db.r['foobaz'] = 'bazbaz'
    db.r['goobar'] = 'borbor'
    assert db.r.multi_del('foo') == 2
    assert db.redisdb.get('foobar') is None
    assert db.redisdb.get('foobaz') is None
    assert db.r['goobar'] == 'borbor'


def test_clear_dict_clears_all_key_values(db):
    db.r['tarbush'] = 156.03
    db.r['reform'] = 'C4rxqkcm'

    db.r.clear()

    assert len(db.r) == 0
