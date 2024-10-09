import os
import pytest

from caom2pipe import manage_composable as mc
import test_conf as tc


@pytest.fixture(scope='function')
def test_config():
    mc.StorageName.collection = 'OMM'
    mc.StorageName.scheme = 'cadc'
    mc.StorageName.collection_pattern = '.*'
    test_config = mc.Config()
    test_config.working_directory = tc.THIS_DIR
    test_config.collection = 'OMM'
    test_config.proxy_file_name = 'proxy.pem'
    test_config.proxy_fqn = os.path.join(tc.TEST_DATA_DIR, 'proxy.pem')
    test_config.work_file = 'todo.txt'
    test_config.interval = 10  # minutes
    test_config.logging_level = 'INFO'
    test_config.log_file_directory = tc.TEST_DATA_DIR
    test_config.failure_fqn = f'{tc.TEST_DATA_DIR}/fail.txt'
    test_config.failure_log_file_name = 'fail.txt'
    test_config.retry_fqn = f'{tc.TEST_DATA_DIR}/retry.txt'
    test_config.retry_file_name = 'retry.txt'
    test_config.state_file_name = 'state.yml'
    test_config.success_fqn = f'{tc.TEST_DATA_DIR}/good.txt'
    test_config.success_log_file_name = 'good.txt'
    test_config.rejected_fqn = f'{tc.TEST_DATA_DIR}/rejected.yml'
    test_config.progress_fqn = f'{tc.TEST_DATA_DIR}/progress.txt'
    test_config.resource_id = 'ivo://cadc.nrc.ca/sc2repo'
    test_config._report_fqn = (
        f'{test_config.log_file_directory}/' f'test_report.txt'
    )
    test_config.storage_inventory_resource_id = 'ivo://cadc.nrc.ca/TEST'
    for f_name in [
        test_config.failure_fqn,
        test_config.success_fqn,
        test_config.retry_fqn,
    ]:
        if os.path.exists(f_name):
            os.unlink(f_name)
    test_config.time_zone = 'UTC'
    return test_config


@pytest.fixture(scope='function')
def test_data_dir():
    this_dir = os.path.dirname(os.path.realpath(__file__))
    return os.path.join(this_dir, 'data')


@pytest.fixture(scope='function')
def test_files_dir():
    return '/test_files'


@pytest.fixture()
def change_test_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
