import uuid

from floorist.floorist import main
from floorist.config import Config
from pandas import DataFrame


def test_floorplan_without_prefix_raises_exception_keeps_reading_other_floorplans(mocker):

    mocker.patch('floorist.floorist.open')
    mocker.patch('floorist.floorist.logging')

    config_mock = mocker.patch('floorist.floorist.get_config')
    config_mock.return_value = Config(bucket_name='foo')

    awswrangler_mock = mocker.patch('floorist.floorist.wr')

    connection_engine_mock = mocker.patch('floorist.floorist.create_engine')
    connection_mock = connection_engine_mock().connect().execution_options()

    exit_mock = mocker.patch('floorist.floorist.exit')

    safe_load_mock = mocker.patch('floorist.floorist.yaml.safe_load')
    safe_load_mock.return_value = [{'query': "a query", 'prefix': None}, {'query': 'another-query', 'prefix': 'a prefix'}]

    pandas_mock = mocker.patch('floorist.floorist.pd')
    data_stub = DataFrame({
        'ID': [uuid.uuid4(), uuid.uuid4(), uuid.uuid4()],
        'columnA': ["foo", "bar", "baz"]
    })
    pandas_mock.read_sql.return_value = [data_stub]

    main()

    pandas_mock.read_sql.assert_called_once_with("another-query", connection_mock, chunksize=1000)
    data_stub.equals(awswrangler_mock.s3.to_parquet.call_args[0])
    exit_mock.assert_called_once_with(1)
