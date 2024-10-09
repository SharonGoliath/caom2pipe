# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import glob
import os

from astropy.io import fits
from astropy.table import Table
from collections import defaultdict, deque
from datetime import datetime, timedelta
from dateutil import tz

from unittest.mock import call, Mock, patch, PropertyMock
import test_conf as tc

from cadcdata import FileInfo
from cadcutils import exceptions
from caom2 import SimpleObservation, Algorithm
from caom2pipe.client_composable import ClientCollection
from caom2pipe import data_source_composable as dsc
from caom2pipe import execute_composable as ec
from caom2pipe.html_data_source import HttpDataSource, HtmlFilteredPagesTemplate
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc
from caom2pipe.reader_composable import DelayedClientReader, FileMetadataReader, Hdf5FileMetadataReader
from caom2pipe import run_composable as rc
from caom2pipe import name_builder_composable as b
from caom2pipe.transfer_composable import CadcTransfer

import test_execute_composable
import visit_mock

STATE_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_state.yml')
TEST_DIR = f'{tc.TEST_DATA_DIR}/run_composable'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.manage_composable.read_obs_from_file')
@patch('caom2pipe.manage_composable.write_obs_to_file')
def test_run_todo_list_dir_data_source(
    write_obs_mock,
    read_obs_mock,
    visit_meta_mock,
    clients_mock,
    test_config,
    tmpdir,
):
    test_dir = f'{tmpdir}/abc'
    os.mkdir(test_dir)
    with open(f'{test_dir}/abc.xml', 'w') as f:
        f.write('test content')

    read_obs_mock.side_effect = _mock_read
    test_config.change_working_directory(tmpdir)
    test_config.use_local_files = True
    test_config.data_sources = ['/test_files/sub_directory']
    test_config.data_source_extensions = ['.fits']
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.logging_level = 'INFO'
    test_result = rc.run_by_todo(config=test_config)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert visit_meta_mock.called, 'expect visit call'
    visit_meta_mock.assert_called_with()
    assert read_obs_mock.called, 'read_obs not called'
    assert write_obs_mock.called, 'write_obs mock not called'
    assert not (
        clients_mock.return_value.metadata_client.read.called
    ), 'scrape, should be no client access'
    assert not (
        clients_mock.return_value.data_client.get_file.called
    ), 'scrape, should be no client access'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_todo_list_dir_data_source_invalid_fname(clients_mock, test_config, tmpdir):
    test_fqn = os.path.join(tmpdir, 'abc.fits.gz')
    with open(test_fqn, 'w') as f:
        f.write('abc')

    test_config.change_working_directory(tmpdir)
    test_config.use_local_files = True
    test_config.data_sources = [tmpdir]
    test_config.data_source_extensions = ['.fits', '.fits.gz']
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.log_to_file = False

    class TStorageName(mc.StorageName):
        def __init__(self, entry):
            super().__init__(obs_id=os.path.basename(entry))
            self._source_names = [entry]

        def is_valid(self):
            return False

    class TStorageNameInstanceBuilder(b.StorageNameInstanceBuilder):
        def __init__(self):
            pass

        def build(self, entry):
            return TStorageName(entry)

    test_builder = TStorageNameInstanceBuilder()
    test_chooser = ec.OrganizeChooser()
    data_source = dsc.ListDirSeparateDataSource(test_config)
    test_result = rc.run_by_todo(config=test_config, chooser=test_chooser, sources=[data_source], name_builder=test_builder)
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure, because of file naming'
    assert os.path.exists(test_config.failure_fqn), 'expect failure file'
    assert os.path.exists(test_config.retry_fqn), 'expect retry file'
    assert (
        not clients_mock.metadata_client.read.called
    ), 'repo client read access happens after is_valid call'
    assert not (
        clients_mock.data_client.get_file.called
    ), 'bad file naming, should be no client access'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_run_todo_file_data_source(clients_mock, test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_config.proxy_fqn = 'test_proxy.pem'
    clients_mock.return_value.metadata_client.read.side_effect = Mock(
        return_value=SimpleObservation(
            collection=test_config.collection,
            observation_id='def',
            algorithm=Algorithm('test'),
        )
    )

    test_config.work_fqn = f'{tmpdir}/todo.txt'
    with open(test_config.work_fqn, 'w') as f:
        f.write('def.fits.gz\n')

    test_config.task_types = [mc.TaskType.VISIT]
    test_config.log_to_file = True
    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_todo(config=test_config, chooser=test_chooser)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert os.path.exists(test_config.success_fqn), 'expect success file'

    with open(test_config.success_fqn) as f:
        content = f.read()
        # the obs id and file name
        assert 'def def.fits' in content, 'wrong success message'
    assert clients_mock.return_value.metadata_client.read.called, 'expect e call'
    clients_mock.return_value.metadata_client.read.assert_called_with('OMM', 'def'), 'wrong e args'


@patch('caom2pipe.data_source_composable.QueryTimeBoxDataSource')
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.data_source_composable.CadcTapClient')
@patch('caom2pipe.execute_composable.MetaVisit._visit_meta')
def test_run_state(
    visit_meta_mock,
    tap_mock,
    clients_mock,
    data_source_mock,
    test_config,
    tmpdir,
):
    # tap_mock is used by the data_source_composable class
    visit_meta_mock.side_effect = _mock_visit
    clients_mock.return_value.metadata_client.read.side_effect = _mock_read2

    state_test_end_time = datetime.fromtimestamp(1579740838)
    state_test_start_time = state_test_end_time - timedelta(seconds=900)
    start_time_mock = PropertyMock(return_value=state_test_start_time)
    type(data_source_mock.return_value).start_dt = start_time_mock
    end_time_mock = PropertyMock(return_value=state_test_end_time)
    type(data_source_mock.return_value).end_dt = end_time_mock
    data_source_mock.return_value.get_time_box_work.side_effect = _mock_query2

    orig_getcwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        test_config.change_working_directory(tmpdir)
        test_config.task_types = [mc.TaskType.INGEST]
        test_config.interval = 10
        # use_local_files set so run_by_state chooses QueryTimeBoxDataSource
        test_config.use_local_files = False
        mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, state_test_start_time)
        individual_log_file = f'{test_config.log_file_directory}/NEOS_SCI_2015347000000_clean.log'
        test_chooser = ec.OrganizeChooser()
        test_reader = Mock()
        test_result = rc.run_by_state(config=test_config, chooser=test_chooser, metadata_reader=test_reader)
        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'expect success'
        assert visit_meta_mock.called, 'expect visit meta call'
        visit_meta_mock.assert_called_once_with()
        assert test_reader.reset.called, 'expect reset call'
        assert test_reader.reset.call_count == 1, 'wrong call count'

        assert data_source_mock.return_value.save_start_dt.called, 'save_start_dt should be called'
        data_source_mock.return_value.save_start_dt.assert_called_with(state_test_end_time), 'wrong time'
        assert os.path.exists(test_config.progress_fqn), 'expect progress file'
        assert os.path.exists(test_config.success_fqn), 'log_to_file set to false, no success file'
        assert not os.path.exists(individual_log_file), f'log_to_file is False, no entry log'

        # test that runner does nothing when times haven't changed
        start_time = state_test_end_time
        mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, start_time)
        visit_meta_mock.reset_mock()
        test_result = rc.run_by_state(config=test_config, chooser=test_chooser, metadata_reader=test_reader)
        assert test_result is not None, 'expect a result'
        assert test_result == 0, 'expect success'
        assert not visit_meta_mock.called, 'expect no visit_meta call'
        assert test_reader.reset.called, 'expect reset call'
        assert test_reader.reset.call_count == 1, 'wrong call count'
        assert data_source_mock.return_value.save_start_dt.called, 'save_start_dt should be called'
        data_source_mock.return_value.save_start_dt.assert_called_with(state_test_end_time), 'wrong time'
    finally:
        os.chdir(orig_getcwd)


@patch('caom2pipe.data_source_composable.CadcTapClient')
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.client_composable.query_tap_client')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
def test_run_state_log_to_file_true(
    visit_meta_mock,
    tap_mock,
    clients_mock,
    tap_mock2,
    test_config,
    tmpdir,
):
    # tap_mock2 is needed by the data_source_composable specialization
    # this test is about making sure the summary .txt files are copied
    # as expected when there is more than one time-box

    global call_count
    call_count = 0

    test_config.change_working_directory(tmpdir)
    if not os.path.exists(os.path.dirname(test_config.success_fqn)):
        os.mkdir(os.path.dirname(test_config.success_fqn))

    clients_mock.return_value.metadata_client.read.side_effect = Mock(
        return_value=SimpleObservation(
            collection=test_config.collection,
            observation_id='def',
            algorithm=Algorithm('test'),
        )
    )
    visit_meta_mock.side_effect = _mock_visit
    tap_mock.side_effect = _mock_query_table2

    test_end_time = datetime.fromtimestamp(1579740838)
    start_time = test_end_time - timedelta(seconds=900)
    mc.State.write_bookmark(STATE_FILE, test_config.bookmark, start_time)

    test_config.task_types = [mc.TaskType.INGEST]
    test_config.log_to_file = True
    test_config.state_fqn = STATE_FILE
    test_config.interval = 10
    pattern = f'{test_config.success_fqn.split(".")[0]}*'

    if os.path.exists(test_config.progress_fqn):
        os.unlink(test_config.progress_fqn)

    # preconditions for the success file: - only one file named pattern.txt
    #
    original_success_files = glob.glob(pattern)
    for entry in original_success_files:
        os.unlink(entry)
    if not os.path.exists(test_config.success_fqn):
        with open(test_config.success_fqn, 'w') as f:
            f.write('test content\n')

    test_chooser = ec.OrganizeChooser()
    test_result = rc.run_by_state(config=test_config, chooser=test_chooser)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert os.path.exists(test_config.progress_fqn), 'expect progress file'
    file_count = glob.glob(pattern)
    # 3 = 2 iterations through the state information, + 1 existing success file
    # one of the iterations through the state information processes one file
    assert len(file_count) == 3, 'wrong number of success files'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_todo_list_dir_data_source_exception(
    do_one_mock, clients_mock, test_config, tmpdir
):
    test_config.change_working_directory(tmpdir)
    test_config.use_local_files = True
    test_config.task_types = [mc.TaskType.SCRAPE]
    test_config.log_to_file = True

    with open(f'{tmpdir}/abc.fits.gz', 'w') as f:
        f.write('test content\n')

    do_one_mock.side_effect = mc.CadcException

    test_chooser = ec.OrganizeChooser()
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_data_source = dsc.ListDirDataSource(test_config, test_chooser)
    test_data_source.reporter = test_reporter
    test_result = rc.run_by_todo(
        config=test_config,
        chooser=test_chooser,
        sources=[test_data_source],
    )
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure'
    assert do_one_mock.called, 'expect do_one call'
    assert os.path.exists(test_config.failure_fqn), 'expect failure file'
    assert os.path.exists(test_config.retry_fqn), 'expect retry file'

    with open(test_config.failure_fqn) as f:
        content = f.read()
        # the obs id and file name
        assert 'abc abc.fits' in content, 'wrong failure message'

    with open(test_config.retry_fqn) as f:
        content = f.read()
        # retry file names
        assert content == 'abc.fits\n', 'wrong retry content'

    assert not (
        clients_mock.return_value.metadata_client.read.called
    ), 'scrape, should be no metadata client call'
    assert not (
        clients_mock.return_value.data_client.get_file_info.called
    ), 'scrape, should be no data client call'


@patch('caom2pipe.data_source_composable.TodoFileDataSource.clean_up')
@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
def test_run_todo_retry(do_one_mock, clients_mock, source_mock, test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_config.log_to_file = True
    test_config.retry_failures = True
    test_config.retry_decay = 0
    test_config.task_types = [mc.TaskType.INGEST]
    _write_todo(test_config)
    retry_success_fqn = f'{tmpdir}/logs_0/{test_config.success_log_file_name}'
    retry_failure_fqn = f'{tmpdir}/logs_0/{test_config.failure_log_file_name}'
    retry_retry_fqn = f'{tmpdir}/logs_0/{test_config.retry_file_name}'
    do_one_mock.side_effect = _mock_do_one_failure

    test_result = rc.run_by_todo(config=test_config)

    # what should happen when a failure execution occurs
    assert test_result is not None, 'expect a result'
    assert test_result == -1, 'expect failure'
    _check_log_files_for_failure(test_config, retry_success_fqn, retry_failure_fqn, retry_retry_fqn)
    assert do_one_mock.called, 'expect do_one call'
    assert do_one_mock.call_count == 2, 'wrong number of calls'

    assert not (
        clients_mock.return_value.metadata_client.read.called
    ), 'do_one is mocked, should be no metadata client call'
    assert not (
        clients_mock.return_value.data_client.get_file_info.called
    ), 'do_one is mocked, should be no data client call'
    assert source_mock.called, 'clean_up should be called'
    assert source_mock.call_count == 2, 'clean_up should be called two times'
    calls = [
        call('test_obs_id.fits.gz', -1, 0),
        call('test_obs_id.fits.gz', -1, 1),
    ]
    source_mock.assert_has_calls(calls)

    # what should happen when successful execution occurs
    source_mock.reset_mock()
    do_one_mock.reset_mock()
    do_one_mock.side_effect = None
    do_one_mock.return_value = (0, None)
    test_result = rc.run_by_todo(config=test_config)

    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert do_one_mock.called, 'expect do_one call'
    assert do_one_mock.call_count == 1, 'wrong number of calls'
    source_mock.assert_called_with('test_obs_id.fits.gz', 0, 0)


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
@patch('caom2pipe.execute_composable.OrganizeExecutes.do_one')
@patch('caom2pipe.data_source_composable.QueryTimeBoxDataSource')
def test_run_state_retry(ds_mock, do_one_mock, clients_mock, test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_start_time = mc.get_now()
    test_end_time = test_start_time + timedelta(hours=1)
    mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, test_start_time)
    type(ds_mock.return_value).start_dt = PropertyMock(return_value=test_start_time)
    type(ds_mock.return_value).end_dt = PropertyMock(return_value=test_end_time)

    retry_success_fqn = f'{tmpdir}/logs_0/{test_config.success_log_file_name}'
    retry_failure_fqn = f'{tmpdir}/logs_0/{test_config.failure_log_file_name}'
    retry_retry_fqn = f'{tmpdir}/logs_0/{test_config.retry_file_name}'

    global call_count
    call_count = 0
    ds_mock.return_value.get_time_box_work.side_effect = _mock_get_work
    # a failure part-way through an incremental run, followed by a successful execution
    do_one_mock.side_effect = [mc.CadcException, 0]

    test_config.log_to_file = True
    test_config.retry_failures = True
    test_config.retry_count = 1
    test_config.retry_decay = 0
    test_config.interval = 10
    test_config.logging_level = 'DEBUG'
    test_config.task_types = [mc.TaskType.INGEST]
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        mc.Config.write_to_file(test_config)
        test_result = rc.run_by_state(config=test_config)

        assert test_result is not None, 'expect a result'
        assert test_result == -1, 'expect failure'
        _check_log_files_for_failure(test_config, retry_success_fqn, retry_failure_fqn, retry_retry_fqn)
        assert do_one_mock.called, 'expect do_one call'
        assert do_one_mock.call_count == 2, 'wrong number of calls'
    finally:
        os.chdir(orig_cwd)


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_time_box(clients_mock, test_config, tmpdir):
    # test that the time boxes increment as expected, to a maximum value
    # time-boxing is handled by DataSource specializations, and is in the time zone of the data source
    # end timestamp is handled by StateRunner

    test_config.change_working_directory(tmpdir)

    test_config.interval = 700
    test_config.task_types = [mc.TaskType.INGEST]

    for test_tz in [tz.UTC, tz.gettz('US/Mountain')]:
        test_start_time = datetime(2019, 7, 23, 9, 51)
        mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, test_start_time)
        test_end_time = datetime(2019, 7, 24, 9, 20)

        class MakeTimeBoxWork(dsc.IncrementalDataSource):

            def __init__(self):
                super().__init__(test_config, test_config.bookmark)
                self.todo_call_count = 0
                self.zero_called = False
                self.one_called = False
                self.two_called = False

            def _initialize_end_dt(self):
                self._end_dt = test_end_time

            def get_time_box_work(self, prev_exec_dt, exec_dt):
                if self.todo_call_count == 0:
                    assert prev_exec_dt == datetime(2019, 7, 23, 9, 51), f'prev 0 {test_tz}'
                    assert exec_dt == datetime(2019, 7, 23, 21, 31), 'wrong exec 0'
                    self.zero_called = True
                elif self.todo_call_count == 1:
                    assert prev_exec_dt == datetime(2019, 7, 23, 21, 31), 'wrong prev 1'
                    assert exec_dt == datetime(2019, 7, 24, 9, 11), 'wrong exec 1'
                    self.one_called = True
                elif self.todo_call_count == 2:
                    assert prev_exec_dt == datetime(2019, 7, 24, 9, 11), 'wrong exec 2'
                    assert exec_dt == test_end_time, f'wrong exec 2 {test_tz}'
                    self.two_called = True
                self.todo_call_count += 1
                assert self.todo_call_count <= 4, 'loop is written wrong'
                return deque()

        test_work = MakeTimeBoxWork()

        test_result = rc.run_by_state(test_config, meta_visitors=None, data_visitors=None, sources=[test_work])
        assert test_result is not None, 'expect a result'
        assert test_work.zero_called, 'missed zero'
        assert test_work.one_called, 'missed one'
        assert test_work.two_called, 'missed two'
        test_state = mc.State(test_config.state_fqn, test_config.time_zone)
        assert test_state.get_bookmark(test_config.bookmark) == test_end_time
        assert test_work.todo_call_count == 3, 'wrong todo call count'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_time_box_equal(clients_mock, test_config, tmpdir):
    # test that if the end datetime is the same as the start datetime, there are no calls
    test_config.change_working_directory(tmpdir)
    test_config.interval = 700
    test_config.task_types = [mc.TaskType.INGEST]

    test_start_time = test_end_time = datetime(2019, 7, 23, 9, 51)
    mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, test_start_time)

    class MakeWork(dsc.IncrementalDataSource):

        def __init__(self):
            super().__init__(test_config, test_config.bookmark)
            self.todo_call_count = 0

        def _initialize_end_dt(self):
            self._end_dt = test_start_time

        def get_time_box_work(self, prev_exec_dt, exec_dt):
            self.todo_call_count += 1
            assert self.todo_call_count <= 4, 'loop is written wrong'
            return deque()

    test_work = MakeWork()
    test_result = rc.run_by_state(test_config, meta_visitors=None, data_visitors=None, sources=[test_work])
    assert test_result is not None, 'expect a result'
    test_state = mc.State(test_config.state_fqn, test_config.time_zone)
    assert test_state.get_bookmark(test_config.bookmark) == test_end_time
    assert test_work.todo_call_count == 0, 'wrong todo call count'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_time_box_once_through(clients_mock, test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_config.interval = 700
    test_config.task_types = [mc.TaskType.INGEST]

    for test_timezone in [tz.UTC, tz.gettz('US/Mountain')]:
        test_start_time = datetime(2019, 7, 23, 9, 51)
        mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, test_start_time)
        test_end_time = datetime(2019, 7, 23, 12, 20)

        class MakeWork(dsc.IncrementalDataSource):

            def __init__(self):
                super().__init__(test_config, test_config.bookmark)
                self.todo_call_count = 0
                self.zero_called = False

            def _initialize_end_dt(self):
                self._end_dt = test_end_time

            def get_time_box_work(self, prev_exec_dt, exec_dt):
                if self.todo_call_count == 0:
                    assert prev_exec_dt == datetime(2019, 7, 23, 9, 51), 'prev'
                    assert exec_dt == datetime(2019, 7, 23, 12, 20), 'wrong exec'
                    self.zero_called = True
                self.todo_call_count += 1
                assert self.todo_call_count <= 4, 'loop is written wrong'
                return deque()

        test_work = MakeWork()
        test_result = rc.run_by_state(test_config, meta_visitors=None, data_visitors=None, sources=[test_work])
        assert test_result is not None, 'expect a result'

        test_state = mc.State(test_config.state_fqn, test_config.time_zone)
        assert test_work.zero_called, 'missed zero'
        assert test_state.get_bookmark(test_config.bookmark) == test_end_time
        assert test_work.todo_call_count == 1, 'wrong todo call count'


@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.reader_composable.FileMetadataReader._retrieve_headers')
@patch('caom2pipe.reader_composable.FileMetadataReader._retrieve_file_info')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._move_action')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource.get_work')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_store_ingest_failure(
    data_client_mock,
    repo_client_mock,
    get_work_mock,
    cleanup_mock,
    reader_file_info_mock,
    reader_headers_mock,
    access_mock,
    visit_meta_mock,
    caom2_store_mock,
    test_config,
    tmpdir,
):
    access_mock.return_value = 'https://localhost'
    temp_deque = deque()
    temp_deque.append('/data/dao_c122_2021_005157_e.fits')
    temp_deque.append('/data/dao_c122_2021_005157.fits')
    get_work_mock.return_value = temp_deque
    repo_client_mock.return_value.read.return_value = None
    reader_headers_mock.return_value = [{'OBSMODE': 'abc'}]

    def _file_info_mock(key, ignore):
        return FileInfo(
            id=key,
            file_type='application/fits',
            md5sum='md5:def',
        )

    # this is the exception raised in data_util.StorageClientWrapper, as
    # the mc.CadcException definition is not available to that package
    data_client_mock.return_value.put.side_effect = (
        exceptions.UnexpectedException
    )
    # mock a series of failures with the CADC storage service
    data_client_mock.return_value.info.side_effect = (
        exceptions.UnexpectedException
    )
    reader_file_info_mock.side_effect = _file_info_mock
    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.use_local_files = True
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.data_sources = ['/data']
    test_config.data_source_extensions = ['.fits']
    test_config.logging_level = 'INFO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.write_to_file(test_config)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    data_source = dsc.LocalFilesDataSource(
        test_config,
        data_client_mock,
        Mock(),
    )
    test_result = rc.run_by_todo(sources=[data_source])
    assert test_result is not None, 'expect result'
    assert test_result == -1, 'expect failure'
    # execution stops before this call should be made
    assert not repo_client_mock.return_value.read.called, 'no read'
    # make sure data is not really being written to CADC storage :)
    assert (
        data_client_mock.return_value.put.called
    ), 'put should be called'
    assert (
        data_client_mock.return_value.put.call_count == 2
    ), 'wrong number of puts'
    put_calls = [
        call('/data', 'cadc:OMM/dao_c122_2021_005157_e.fits'),
        call('/data', 'cadc:OMM/dao_c122_2021_005157.fits'),
    ]
    data_client_mock.return_value.put.assert_has_calls(
        put_calls, any_order=False
    )
    assert cleanup_mock.called, 'cleanup'
    cleanup_calls = [
        call('/data/dao_c122_2021_005157_e.fits', '/data/failure'),
        call('/data/dao_c122_2021_005157.fits', '/data/failure'),
    ]
    cleanup_mock.assert_has_calls(cleanup_calls), 'wrong cleanup args'
    assert not visit_meta_mock.called, 'no _visit_meta call'
    assert not caom2_store_mock.called, 'no _caom2_store call'
    assert not reader_file_info_mock.called, 'info'
    assert not reader_headers_mock.called, 'get_head should be called'


@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_store_fitsverify_failure(
    clients_mock,
    visit_meta_mock,
    caom2_store_mock,
    test_config,
    tmpdir,
):
    # check that the default DataSource behaviour (LocalFilesDataSource) calls fitsverify
    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.use_local_files = True
    test_config.cleanup_files_when_storing = False
    test_config.data_sources = ['/test_files/sub_directory_broken']
    test_config.data_source_extensions = ['.fits']
    test_config.logging_level = 'DEBUG'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.retry_failures = True
    test_config.write_to_file(test_config)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    test_result = rc.run_by_todo()
    assert test_result is not None, 'expect result'
    assert test_result == 0, 'expect success'
    # execution stops before this call should be made
    assert not clients_mock.return_value.metadata_client.read.called, 'no read'
    assert not clients_mock.return_value.data_client.put.called, 'put should be called'
    assert clients_mock.return_value.data_client.put.call_count == 0, 'wrong number of puts'
    assert not visit_meta_mock.called, 'no _visit_meta call'
    assert not caom2_store_mock.called, 'no _caom2_store call'
    test_report = mc.ExecutionSummary.read_report_file(test_config.report_fqn)
    assert test_report is not None, 'expect a text report'
    assert test_report._rejected_sum == 1, 'fitsverify rejection'
    assert test_report._errors_sum == 1, 'fitsverify rejection'
    assert test_report._retry_sum == 0, 'no retries'
    assert test_report._entries_sum == 1, 'found a fits file'
    assert test_report._success_sum == 0, 'did not try to process the broken file'


@patch('caom2utils.data_util.get_local_file_headers')
@patch('caom2utils.data_util.get_local_file_info')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._verify_file')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._move_action')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_store_get_work_failures(
    data_client_mock,
    repo_client_mock,
    cleanup_mock,
    access_mock,
    visit_meta_mock,
    caom2_store_mock,
    check_mock,
    file_info_mock,
    headers_mock,
    test_config,
    tmp_path,
    change_test_dir,
):
    # this is a test that a fitsverify failure will be logged in the failure log,
    # and an md5sum that is the same on disk as at CADC will be logged in the success log

    access_mock.return_value = 'https://localhost'
    repo_client_mock.return_value.read.return_value = None

    def _file_info_mock(uri):
        return FileInfo(
            id=uri,
            file_type='application/fits',
            md5sum='md5:d937df477fe2511995fa39a027e8ce2f',
        )

    data_client_mock.info.side_effect = _file_info_mock
    test_config.change_working_directory(tmp_path)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.use_local_files = True
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.store_modified_files_only = True
    test_config.data_sources = ['/data']
    test_config.data_source_extensions = ['.fits']
    test_config.logging_level = 'INFO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.recurse_data_sources = False

    test_config.write_to_file(test_config)

    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    test_end_time = datetime.fromtimestamp(1579740838)
    start_time = test_end_time - timedelta(seconds=900)
    mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, start_time)

    stat_return_value = type('', (), {})
    stat_return_value.st_mtime = 1579740835.7357888
    dir_entry_1 = type('', (), {})
    dir_entry_1.name = 'a2020_06_17_07_00_01.fits'
    dir_entry_1.path = '/test_files/a2020_06_17_07_00_01.fits'
    dir_entry_1.stat = Mock(return_value=stat_return_value)
    dir_entry_1.is_dir = Mock(return_value=False)
    dir_entry_2 = type('', (), {})
    dir_entry_2.name = 'a2022_07_26_05_50_01.fits'
    dir_entry_2.path = '/test_files/a2022_07_26_05_50_01.fits'
    dir_entry_2.stat = Mock(return_value=stat_return_value)
    dir_entry_2.is_dir = Mock(return_value=False)

    check_mock.side_effect = [True, False]

    file_metadata_reader = FileMetadataReader()
    file_info_mock.side_effect = lambda x: FileInfo(
        id=x, md5sum='d937df477fe2511995fa39a027e8ce2f', file_type='application/fits'
    )

    # minimal header for testing
    h = fits.Header()
    h['SIMPLE'] = 'T'
    h['BITPIX'] = -32
    h['NAXIS'] = 2
    h['NAXIS1'] = 2048
    h['NAXIS2'] = 2048
    h['DATATYPE'] = 'REDUC'
    h['TYPE'] = 'image '
    test_headers = [h]
    headers_mock.return_value = test_headers

    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = [dir_entry_1, dir_entry_2]
        data_source = dsc.LocalFilesDataSource(test_config, data_client_mock, file_metadata_reader)
        data_source.reporter = test_reporter
        type(data_source).end_dt = PropertyMock(return_value=test_end_time)
        clients_mock = ClientCollection(test_config)
        clients_mock._data_client = data_client_mock
        clients_mock._metadata_client = repo_client_mock
        test_result = rc.run_by_state(
            sources=[data_source], clients=clients_mock, metadata_reader=file_metadata_reader
        )

        assert test_result is not None, 'expect result'
        assert test_result == 0, 'expect successful execution'
        # execution stops before this call should be made
        assert not repo_client_mock.return_value.read.called, 'no read'
        # make sure data is not really being written to CADC storage :)
        assert not data_client_mock.put.called, 'put should not be called'
        assert cleanup_mock.called, 'cleanup'
        cleanup_calls = [
            call('/test_files/a2020_06_17_07_00_01.fits', '/data/success'),
            call('/test_files/a2022_07_26_05_50_01.fits', '/data/failure'),
        ]
        cleanup_mock.assert_has_calls(cleanup_calls), 'wrong cleanup args'
        assert not visit_meta_mock.called, 'no _visit_meta call'
        assert not caom2_store_mock.called, 'no _caom2_store call'
        assert os.path.exists(test_config.failure_fqn), f'failure log {test_config.failure_fqn} should exist'
        with open(test_config.failure_fqn, 'r') as f:
            content = f.readlines()
            assert len(content) == 1, 'expect 1 failure'
            assert 'a2022_07_26_05_50_01' in content[0], 'expect verify in failure log'
            assert 'a2020_06_17_07_00_01' not in content[0], 'expect md5sum in success log'

        assert os.path.exists(test_config.success_fqn), f'failure log {test_config.success_fqn} should exist'
        with open(test_config.success_fqn, 'r') as f:
            content = f.readlines()
            assert len(content) == 1, 'expect 1 success'
            assert 'a2020_06_17_07_00_01' in content[0], 'expect md5sum in success log'
            assert 'a2022_07_26_05_50_01' not in content[0], 'expect verify in failure log'


@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('caom2pipe.execute_composable.CaomExecute._caom2_store')
@patch('caom2pipe.execute_composable.CaomExecute._visit_meta')
@patch('caom2pipe.data_source_composable.TodoFileDataSource.get_work')
@patch('caom2pipe.client_composable.CAOM2RepoClient')
@patch('caom2pipe.client_composable.StorageClientWrapper')
def test_run_ingest(
    data_client_mock,
    repo_client_mock,
    data_source_mock,
    meta_visit_mock,
    caom2_store_mock,
    access_url_mock,
    test_config,
    tmpdir
):
    access_url_mock.return_value = 'https://localhost:8080'
    temp_deque = deque()
    test_f_name = '1319558w.fits.fz'
    temp_deque.append(test_f_name)
    data_source_mock.return_value = temp_deque
    repo_client_mock.return_value.read.return_value = None
    data_client_mock.return_value.get_head.return_value = [
        {'INSTRUME': 'WIRCam'},
    ]

    data_client_mock.return_value.info.return_value = FileInfo(
        id=test_f_name,
        file_type='application/fits',
        md5sum='abcdef',
    )

    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.INGEST]
    test_config.logging_level = 'INFO'
    test_config.collection = 'CFHT'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.use_local_files = False
    test_config.write_to_file(test_config)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')

    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_data_source = dsc.TodoFileDataSource(test_config)
    test_data_source.reporter = test_reporter
    test_result = rc.run_by_todo(sources=[test_data_source])
    assert test_result is not None, 'expect result'
    assert test_result == 0, 'expect success'
    assert repo_client_mock.return_value.read.called, 'read called'
    assert data_client_mock.return_value.info.called, 'info'
    assert (
        data_client_mock.return_value.info.call_count == 1
    ), 'wrong number of info calls'
    data_client_mock.return_value.info.assert_called_with(f'cadc:CFHT/{test_f_name}')
    assert (
        data_client_mock.return_value.get_head.called
    ), 'get_head should be called'
    assert (
        data_client_mock.return_value.get_head.call_count == 1
    ), 'wrong number of get_heads'
    data_client_mock.return_value.get_head.assert_called_with(f'cadc:CFHT/{test_f_name}')
    assert meta_visit_mock.called, '_visit_meta call'
    assert meta_visit_mock.call_count == 1, '_visit_meta call count'
    assert caom2_store_mock.called, '_caom2_store call'
    assert caom2_store_mock.call_count == 1, '_caom2_store call count'
    # get_work is mocked, so Reporter/Summary cannot be checked


@patch('caom2pipe.execute_composable.get_local_file_info')
@patch('caom2pipe.client_composable.vault_info')
@patch('caom2pipe.execute_composable.FitsForCADCDecompressor.fix_compression')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
@patch('vos.vos.Client')
def test_vo_with_cleanup(
    vo_client_mock,
    access_mock,
    fix_mock,
    vault_info_mock,
    local_file_info_mock,
    test_config,
    tmpdir
):
    access_mock.return_value = 'https://localhost'
    test_obs_id = 'sky_cam_image'
    test_f_name = f'{test_obs_id}.fits.gz'
    vo_client_mock.listdir.return_value = ['sky_cam_image.fits.gz']
    vo_client_mock.isdir.return_value = False
    vo_client_mock.status.return_value = False
    test_file_info = FileInfo(
        id=test_f_name,
        file_type='application/fits',
        md5sum='abcdef',
    )
    vault_info_mock.return_value = test_file_info
    local_file_info_mock.return_value = test_file_info
    clients_mock = Mock()
    clients_mock.vo_client = vo_client_mock
    clients_mock.data_client.info.side_effect = [None, test_file_info]
    store_transfer_mock = Mock(autospec=True)
    fix_mock.return_value = f'{tmpdir}/{test_f_name}'
    test_config.change_working_directory(tmpdir)
    test_config.task_types = [mc.TaskType.STORE]
    test_config.logging_level = 'INFO'
    test_config.collection = 'DAO'
    test_config.proxy_file_name = 'cadcproxy.pem'
    test_config.data_sources = ['vos:goliaths/DAOTest']
    test_config.data_source_extensions = ['.fits.gz']
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = 'vos:goliaths/DAOTest/fail'
    test_config.cleanup_success_destination = 'vos:goliaths/DAOTest/pass'
    test_config.store_modified_files_only = True
    test_config.write_to_file(test_config)
    test_builder = nbc.GuessingBuilder(mc.StorageName)
    with open(test_config.proxy_fqn, 'w') as f:
        f.write('test content')
    # execution
    test_result = rc.run_by_todo(
        clients=clients_mock,
        store_transfer=store_transfer_mock,
        name_builder=test_builder,
    )
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert fix_mock.called, 'fix_compression call'
    fix_mock.assert_called_with(f'{tmpdir}/sky_cam_image/{test_f_name}')
    assert vo_client_mock.move.called, 'vo mock call'
    vo_client_mock.move.assert_called_with(
        'vos:goliaths/DAOTest/sky_cam_image.fits.gz',
        'vos:goliaths/DAOTest/pass/sky_cam_image.fits.gz',
    ), 'move args'
    assert clients_mock.data_client.put.called, 'put call'
    clients_mock.data_client.put.assert_called_with(tmpdir, 'cadc:DAO/sky_cam_image.fits')


def _check_logs(reporter, storage_name, failure_should_exist, retry_should_exist, success_should_exist):
    failure_files = reporter.get_file_names_from_log_file(reporter._failure_fqn)
    retry_files = reporter.get_file_names_from_log_file(reporter._retry_fqn)
    success_files = reporter.get_file_names_from_log_file(reporter._success_fqn)
    if failure_should_exist:
        assert storage_name.file_name in failure_files, 'should be failure logging'
    else:
        assert storage_name.file_name not in failure_files, 'should not be failure logging'
    if retry_should_exist:
        assert (
            storage_name.source_names[0] in retry_files
            or storage_name.file_name in retry_files
        ), 'should be retry logging'
    else:
        assert not (
            storage_name.source_names[0] in retry_files
            and storage_name.file_name in retry_files
        ), 'should not be retry logging'
    if success_should_exist:
        assert storage_name.file_name in success_files, 'should be success logging'
    else:
        assert storage_name.file_name not in success_files, 'should not be success logging'


def _check_log_files_for_failure(test_config, retry_success_fqn, retry_failure_fqn, retry_retry_fqn):
    assert os.path.exists(test_config.success_fqn), 'empty success file'
    success_size = mc.get_file_size(test_config.success_fqn)
    assert success_size == 0, 'empty success file'
    assert os.path.exists(test_config.failure_fqn), 'expect failure file'
    assert os.path.exists(test_config.retry_fqn), 'expect retry file'
    assert os.path.exists(retry_success_fqn), f'empty success file {retry_success_fqn}'
    success_size = mc.get_file_size(retry_success_fqn)
    assert success_size == 0, 'empty success file'
    assert os.path.exists(retry_failure_fqn), 'expect failure file'
    assert os.path.exists(retry_retry_fqn), 'expect retry file'


@patch('caom2pipe.client_composable.ClientCollection', autospec=True)
def test_time_box_time_zones(clients_mock, test_config, tmpdir):
    # test that the time boxes increment as expected, to a maximum value
    # time-boxing is handled by DataSource specializations, and is in the time zone of the data source
    # end timestamp is handled by StateRunner

    test_config.change_working_directory(tmpdir)

    test_config.interval = 700
    test_config.task_types = [mc.TaskType.INGEST]

    for test_tz in [tz.UTC, tz.gettz('US/Mountain')]:
        test_start_time = datetime(2019, 7, 23, 9, 51)
        mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, test_start_time)
        test_end_time = datetime(2019, 7, 24, 9, 20)

        class MakeTimeBoxWork(dsc.IncrementalDataSource):

            def __init__(self):
                super().__init__(test_config, test_config.bookmark)
                self.todo_call_count = 0
                self.zero_called = False
                self.one_called = False
                self.two_called = False

            def _initialize_end_dt(self):
                self._end_dt = test_end_time

            def get_time_box_work(self, prev_exec_dt, exec_dt):
                if self.todo_call_count == 0:
                    assert prev_exec_dt == datetime(2019, 7, 23, 9, 51), f'prev 0 {test_tz}'
                    assert exec_dt == datetime(2019, 7, 23, 21, 31), 'wrong exec 0'
                    self.zero_called = True
                elif self.todo_call_count == 1:
                    assert prev_exec_dt == datetime(2019, 7, 23, 21, 31), 'wrong prev 1'
                    assert exec_dt == datetime(2019, 7, 24, 9, 11), 'wrong exec 1'
                    self.one_called = True
                elif self.todo_call_count == 2:
                    assert prev_exec_dt == datetime(2019, 7, 24, 9, 11), 'wrong exec 2'
                    assert exec_dt == test_end_time, f'wrong exec 2 {test_tz}'
                    self.two_called = True
                self.todo_call_count += 1
                assert self.todo_call_count <= 4, 'loop is written wrong'
                return deque()

        test_work = MakeTimeBoxWork()

        test_result = rc.run_by_state(test_config, meta_visitors=None, data_visitors=None, sources=[test_work])
        assert test_result is not None, 'expect a result'
        assert test_work.zero_called, 'missed zero'
        assert test_work.one_called, 'missed one'
        assert test_work.two_called, 'missed two'
        test_state = mc.State(test_config.state_fqn, test_config.time_zone)
        assert test_state.get_bookmark(test_config.bookmark) == test_end_time
        assert test_work.todo_call_count == 3, 'wrong todo call count'


class IsValidFails(tc.TStorageName):

    def is_valid(self):
        return False


class BuilderWithException(nbc.StorageNameBuilder):

    def build(self, entry):
        raise mc.CadcException('builder fails')


class BuilderWithIsValidFails(nbc.StorageNameBuilder):

    def build(self, entry):
        return IsValidFails()


class BuilderWithNoException(nbc.StorageNameBuilder):

    def build(self, entry):
        return tc.TStorageName()


class TestProcessEntry:
    # TodoRunner._process_entry execution paths

    def _ini(self, test_config, tmp_path):
        test_config.change_working_directory(tmp_path)
        test_config.task_types = [mc.TaskType.INGEST]
        test_meta = [test_execute_composable.VisitNoException()]
        self._reader = Mock()
        self._observer = mc.Observable(test_config)
        self._reporter = mc.ExecutionReporter(test_config, self._observer)
        self._clients = Mock()
        self._data_source = Mock()
        self._organizer = ec.OrganizeExecutes(
            test_config,
            test_meta,
            [],
            metadata_reader=self._reader,
            observable=self._observer,
            clients=self._clients,
        )
        self._storage_name = tc.TStorageName()

    def test_build_raises_exception(self, test_config, tmp_path):
        # NameBuilder.build call raises an Exception
        # retry exists because there could be a service call in the naming code
        self._ini(test_config, tmp_path)
        test_builder = BuilderWithException()
        test_subject = rc.StateRunner(
            test_config,
            self._organizer,
            test_builder,
            self._data_source,
            self._reader,
            self._observer,
            self._reporter,
        )
        test_result = test_subject._process_entry(self._data_source, self._storage_name.file_name, current_count=0)
        assert test_result == -1, 'expect failure'
        _check_logs(
            self._reporter,
            self._storage_name,
            failure_should_exist=True,
            retry_should_exist=True,
            success_should_exist=False,
        )

    def test_is_valid_fails(self, test_config, tmp_path):
        # storage_name.is_valid == False
        self._ini(test_config, tmp_path)
        test_builder = BuilderWithIsValidFails()
        test_subject = rc.StateRunner(
            test_config,
            self._organizer,
            test_builder,
            self._data_source,
            self._reader,
            self._observer,
            self._reporter,
        )
        test_result = test_subject._process_entry(self._data_source, self._storage_name.file_name, current_count=0)
        assert test_result == -1, 'expect failure'
        _check_logs(
            self._reporter,
            self._storage_name,
            failure_should_exist=True,
            retry_should_exist=False,
            success_should_exist=False,
        )

    def test_do_one_raises_exception(self, test_config, tmp_path):
        # storage_name.is_valid == True, do_one raises an exception
        self._ini(test_config, tmp_path)
        test_builder = BuilderWithNoException()
        with patch('caom2pipe.execute_composable.OrganizeExecutes.do_one', side_effect=mc.CadcException):
            test_subject = rc.StateRunner(
                test_config,
                self._organizer,
                test_builder,
                self._data_source,
                self._reader,
                self._observer,
                self._reporter,
            )
            test_result = test_subject._process_entry(self._data_source, self._storage_name.file_name, current_count=0)
            assert test_result == -1, 'expect failure'
            _check_logs(
                self._reporter,
                self._storage_name,
                failure_should_exist=True,
                retry_should_exist=True,
                success_should_exist=False,
            )

    def test_do_one_succeeds(self, test_config, tmp_path):
        # storage_name.is_valid == True, do_one succeeds
        self._ini(test_config, tmp_path)
        test_builder = BuilderWithNoException()
        with patch('caom2pipe.execute_composable.OrganizeExecutes.do_one', return_value=(0, None)):
            test_subject = rc.StateRunner(
                test_config,
                self._organizer,
                test_builder,
                self._data_source,
                self._reader,
                self._observer,
                self._reporter,
            )
            test_result = test_subject._process_entry(self._data_source, self._storage_name.file_name, current_count=0)
            assert test_result == 0, 'expect success'
            _check_logs(
                self._reporter,
                self._storage_name,
                failure_should_exist=False,
                retry_should_exist=False,
                success_should_exist=True,
            )

    def test_do_one_non_zero_return(self, test_config, tmp_path):
        # storage_name.is_valid == True, do_one fails
        self._ini(test_config, tmp_path)
        test_builder = BuilderWithNoException()
        with patch('caom2pipe.execute_composable.OrganizeExecutes.do_one', return_value=(-1, 'No executors')):
            test_subject = rc.StateRunner(
                test_config,
                self._organizer,
                test_builder,
                self._data_source,
                self._reader,
                self._observer,
                self._reporter,
            )
            test_result = test_subject._process_entry(self._data_source, self._storage_name.file_name, current_count=0)
            assert test_result == -1, 'expect failure'
            # success_should_exist == False as it's only set in the do_one implementation
            _check_logs(
                self._reporter,
                self._storage_name,
                failure_should_exist=False,
                retry_should_exist=False,
                success_should_exist=False,
            )


class TestRetry:
    """Test retry for each of the following CaomExecutes specializations, because they each call _caom2_store in a "create the record"
    scenario. The specific failure scenario here is the AlreadyExistsException.
    - MetaVisit
    - NoFheadVisit
    - NoFheadStoreVisit
    """

    meta_visit_config = mc.Config()
    meta_visit_config.task_types = [mc.TaskType.INGEST]
    meta_visit_config.use_local_files = True
    meta_visit_config.cleanup_files_when_storing = False
    meta_visit_config.data_sources = ['/tmp']

    nofhead_visit_config = mc.Config()
    nofhead_visit_config.task_types = [mc.TaskType.INGEST, mc.TaskType.MODIFY]
    nofhead_visit_config.use_local_files = True
    nofhead_visit_config.data_sources = ['/tmp']

    nofheadstore_visit_config = mc.Config()
    nofheadstore_visit_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST, mc.TaskType.MODIFY]
    nofheadstore_visit_config.use_local_files = True
    nofheadstore_visit_config.cleanup_files_when_storing = True
    nofheadstore_visit_config.data_sources = ['/tmp']

    def _ini(self, test_config, test_meta, test_data, tmp_path):
        now_dt = datetime.now()
        end_time = now_dt + timedelta(minutes=5)
        start_time = end_time - timedelta(minutes=10)
        test_config.change_working_directory(tmp_path)
        test_config.collection = 'TEST'
        test_config.logging_level = 'INFO'
        test_config.proxy_file_name = 'testproxy.pem'
        test_config.tap_id = 'ivo://cadc.nrc.ca/sc2tap'
        test_config.interval = 10
        test_config.retry_failures = True
        test_config.retry_decay = 0
        test_config.retry_count = 1
        orig_dir = os.getcwd()
        try:
            os.chdir(tmp_path)
            mc.Config.write_to_file(test_config)
            mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, start_time)
            with open(test_config.proxy_fqn, 'w') as f:
                f.write('test content')

            test_data_source = dsc.QueryTimeBoxDataSource(test_config)
            time_box_work = deque()
            time_box_work.append(dsc.StateRunnerMeta('abc.fits', now_dt))
            test_data_source.get_time_box_work = Mock(return_value = time_box_work)
            test_data_source.initialize_end_dt = Mock()
            type(test_data_source).end_dt = PropertyMock(return_value=end_time)
            test_sources = [test_data_source]
            self._reader = Mock()
            self._observer = mc.Observable(test_config)
            self._reporter = mc.ExecutionReporter(test_config, self._observer)
            self._clients = Mock()
            self._name_builder = BuilderWithNoException()
            self._modify_transfer = CadcTransfer(self._clients.data_client)
            self._store_transfer = Mock()
            self._organizer = ec.OrganizeExecutes(
                config=test_config,
                meta_visitors=test_meta,
                data_visitors=test_data,
                metadata_reader=self._reader,
                observable=self._observer,
                clients=self._clients,
                store_transfer=self._store_transfer,
                modify_transfer=self._modify_transfer,
            )
            self._runner = rc.StateRunner(
                test_config,
                self._organizer,
                self._name_builder,
                test_sources,
                self._reader,
                self._observer,
                self._reporter,
            )
        finally:
            os.chdir(orig_dir)

    def _post(self):
        assert self._reporter._summary.success == 1, 'success'
        assert self._reporter._summary._errors_sum == 1, 'errors'
        assert self._reporter._summary._rejected_sum == 0, 'rejected'
        assert self._reporter._summary._retry_sum == 1, 'retry'
        assert self._reporter._summary._skipped_sum == 0, 'skipped'
        assert self._reporter._summary._timeouts_sum == 0, 'timeout'

    @patch('caom2pipe.data_source_composable.CadcTapClient')
    @patch('caom2pipe.execute_composable.CaomExecute._caom2_store', side_effect=[exceptions.AlreadyExistsException, None])
    def test_meta_visit(self, caom2_store_mock, tap_client_mock, tmp_path):
        test_config = TestRetry.meta_visit_config
        test_meta = [test_execute_composable.VisitNoException()]
        test_data = []
        self._ini(test_config, test_meta, test_data, tmp_path)
        test_result = self._runner.run()
        assert test_result == -1, 'expect failure'
        test_result_retry = self._runner.run_retry()
        assert test_result_retry == 0, 'success on retry'
        assert caom2_store_mock.called, 'create'
        assert caom2_store_mock.call_count == 2, 'create call count'
        assert self._clients.metadata_client.read.called, 'read'
        assert self._clients.metadata_client.read.call_count == 2, 'read calls'
        self._post()

    @patch('caom2pipe.data_source_composable.CadcTapClient')
    @patch('caom2pipe.execute_composable.CaomExecute._caom2_store', side_effect=[exceptions.AlreadyExistsException, None])
    def test_nofhead_visit(self, caom2_store_mock, tap_client_mock, tmp_path):
        test_config = TestRetry.nofhead_visit_config
        test_meta = [test_execute_composable.VisitNoException()]
        test_data = [test_execute_composable.VisitNoException()]
        self._ini(test_config, test_meta, test_data, tmp_path)
        test_result = self._runner.run()
        assert test_result == -1, 'expect failure'
        test_result_retry = self._runner.run_retry()
        assert test_result_retry == 0, 'success on retry'
        assert caom2_store_mock.called, 'create'
        assert caom2_store_mock.call_count == 2, 'create call count'
        assert self._clients.metadata_client.read.called, 'read'
        assert self._clients.metadata_client.read.call_count == 2, 'read calls'
        assert self._clients.data_client.get.called, 'get'
        assert self._clients.data_client.get.call_count == 2, 'get calls'
        self._post()

    @patch('caom2pipe.data_source_composable.CadcTapClient')
    @patch('caom2pipe.execute_composable.CaomExecute._cadc_put')
    @patch('caom2pipe.execute_composable.CaomExecute._caom2_store', side_effect=[exceptions.AlreadyExistsException, None])
    def test_nofheadstore_visit(self, cadc_put_mock, caom2_store_mock, tap_client_mock, tmp_path):
        test_config = TestRetry.nofheadstore_visit_config
        test_meta = [test_execute_composable.VisitNoException()]
        test_data = [test_execute_composable.VisitNoException()]
        self._ini(test_config, test_meta, test_data, tmp_path)
        test_result = self._runner.run()
        assert test_result == -1, 'expect failure'
        test_result_retry = self._runner.run_retry()
        assert test_result_retry == 0, 'success on retry'
        assert caom2_store_mock.called, 'create'
        assert caom2_store_mock.call_count == 2, 'create call count'
        assert self._clients.metadata_client.read.called, 'read'
        assert self._clients.metadata_client.read.call_count == 2, 'read calls'
        assert not self._clients.data_client.get.called, 'get'
        assert cadc_put_mock.called, 'put'
        assert cadc_put_mock.call_count == 2, 'put calls'
        self._post()


@patch('caom2pipe.html_data_source.query_endpoint_session')
@patch('caom2pipe.html_data_source.HttpDataSource.initialize_end_dt', autospec=True)
@patch(
    'caom2pipe.html_data_source.HttpDataSource.end_dt', new_callable=PropertyMock(return_value=datetime(2019, 5, 2))
)
@patch('caom2pipe.transfer_composable.HttpTransfer')
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_state_store_ingest_http_retry(
    client_mock, transferrer_mock, end_dt_mock, endpoint_mock, m2, test_config, tmp_path
):
    # Test store + ingest + modify, DelayedReader, NoFheadStore Executor, HttpDataSource (VLASS)
    # 1 success
    # 2 fails the first time with timeout
    # 3 fails both times

    # minimal header for testing
    h = fits.Header()
    h['SIMPLE'] = 'T'
    h['BITPIX'] = -32
    h['NAXIS'] = 2
    h['NAXIS1'] = 2048
    h['NAXIS2'] = 2048
    h['DATATYPE'] = 'REDUC'
    h['TYPE'] = 'image '
    test_headers = [h]

    def _transferrer_get(ignore, local_fqn):
        with open(local_fqn, 'w') as f:
            f.write('test content')

    transferrer_mock.return_value.get.side_effect = _transferrer_get
    client_mock.data_client.get_head.return_value = test_headers
    client_mock.metadata_client.create.side_effect = [
        None,              # success
        mc.CadcException,  # first failure
        mc.CadcException,  # fails first + retry 1 + retry 2 times, the first time
        None,              # succeeds the second time
        mc.CadcException,  # fails first + retry 1 + retry 2 times, the second time
        mc.CadcException,  # fails first + retry 1 + retry 2 times, the third time
    ]
    test_uri_1 = 'nrao:VLASS/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.image.pbcor.tt0.subim.fits'
    test_uri_2 = 'nrao:VLASS/VLASS1.1.ql.T01t01.J000228-363000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits'
    test_uri_3 = 'nrao:VLASS/VLASS1.1.ql.T27t32.J000228-363000.10.2048.v1.I.iter1.image.pbcor.tt0.rms.subim.fits'
    client_mock.data_client.info.side_effect = lambda x: FileInfo(id=x, md5sum='abc')
    test_config.change_working_directory(tmp_path)
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_url = 'https://localhost:12345'
    test_config.data_sources = [test_url]
    test_config.interval = 1200
    test_config.retry_failures = True
    test_config.retry_decay = 0
    test_config.retry_count = 2
    mc.State.write_bookmark(test_config.state_fqn, test_url, datetime(2019, 4, 29, 12, 34))
    client_mock.metadata_client.read.return_value = None
    orig_getcwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        mc.Config.write_to_file(test_config)
        test_metadata_reader = DelayedClientReader(client_mock.data_client)
        session_mock = Mock()
        test_source = HttpDataSource(test_config, test_url, HtmlFilteredPagesTemplate(test_config), session_mock)
        test_source._work = deque(
            [
                dsc.StateRunnerMeta(f'{test_url}T01t01/{os.path.basename(test_uri_1)}', datetime(2019, 4, 30, 1, 1, 1)),
                dsc.StateRunnerMeta(f'{test_url}T01t01/{os.path.basename(test_uri_2)}', datetime(2019, 4, 30, 2, 2, 2)),
                dsc.StateRunnerMeta(f'{test_url}T27t32/{os.path.basename(test_uri_3)}', datetime(2019, 5, 2)),
            ]
        )
        test_source.initialize_end_dt = Mock(autospec=True)
        test_sources = [test_source]
        test_name_builder = nbc.GuessingBuilder(mc.StorageName)
        test_result = rc.run_by_state(
            config=test_config,
            meta_visitors=[visit_mock],
            sources=test_sources,
            store_transfer=transferrer_mock,
            metadata_reader=test_metadata_reader,
            clients=client_mock,
            name_builder=test_name_builder,
        )
        assert test_result is not None, 'expect result'
        assert test_result == -1, 'expect failures because of the retries'
        test_reporter = test_sources[0].reporter
        assert client_mock.metadata_client.read.called, 'read called'
        # 6 = 1 success + 2 failures + 1 retry success + 1 retry failure + 1 second retry failure
        assert client_mock.metadata_client.read.call_count == 6, 'read call count'
        assert client_mock.metadata_client.create.called, 'create called'
        assert client_mock.metadata_client.create.call_count == 6, 'create call count'
        assert client_mock.data_client.put.called, 'put should be called'
        assert client_mock.data_client.put.call_count == 6, 'wrong number of puts'
        client_mock.data_client.put.assert_called_with(
            f'{tmp_path}/{os.path.basename(test_uri_3).replace(".fits", "")}',
            f'{test_config.scheme}:{test_config.collection}/{os.path.basename(test_uri_3)}'
        )
        assert client_mock.data_client.get_head.called, 'get_head called'
        assert client_mock.data_client.get_head.call_count == 6, 'get_head call count'
        assert client_mock.data_client.info.called, 'info called'
        assert client_mock.data_client.info.call_count == 6, 'info call count'
        assert test_reporter._summary.entries == 3, 'all'
        assert test_reporter._summary._errors_sum == 4, 'errors'
        assert test_reporter._summary._retry_sum == 3, 'retry'
        assert test_reporter._summary.success == 2, 'success'
        assert test_reporter._summary._timeouts_sum == 0, 'timeouts'
        assert test_reporter._summary._rejected_sum == 0, 'rejected'
        assert test_reporter._summary._skipped_sum == 0, 'skipped'
    finally:
        os.chdir(orig_getcwd)


@patch('caom2utils.data_util.get_local_headers_from_fits')
@patch('caom2pipe.reader_composable.Hdf5FileMetadataReader._retrieve_file_info')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._append_work')
@patch(
    'caom2pipe.data_source_composable.LocalFilesDataSource.end_dt',
    new_callable=PropertyMock(return_value=datetime(2019, 5, 2)),
)
@patch('caom2pipe.client_composable.ClientCollection')
def test_run_state_store_ingest_local_retry(
    client_mock, end_dt_mock, time_box_mock, file_info_mock, header_mock, test_config, tmp_path
):
    # Test store + ingest + modify, Hdf5FileMetadataReader, NoFheadStore Executor, LocalFilesDataSource (CFHT-like/DAO)
    # 1 success
    # 2 fails the first time with timeout
    # 3 fails both times
    # 4 rejected
    # 5 skipped

    # minimal header for testing
    h = fits.Header()
    h['SIMPLE'] = 'T'
    h['BITPIX'] = -32
    h['NAXIS'] = 2
    h['NAXIS1'] = 2048
    h['NAXIS2'] = 2048
    h['DATATYPE'] = 'REDUC'
    h['TYPE'] = 'image '
    test_headers = [h]
    header_mock.return_value = test_headers

    client_mock.metadata_client.create.side_effect = [
        None,              # success
        mc.CadcException,  # first failure
        mc.CadcException,  # fails first + retry 1 + retry 2 times, the first time
        None,              # succeeds the second time
        mc.CadcException,  # fails first + retry 1 + retry 2 times, the second time
        mc.CadcException,  # fails first + retry 1 + retry 2 times, the third time
    ]
    test_f_1 = '123456o.fits'
    test_f_2 = '2345678p.fits'
    test_f_3 = 'scatsmth.flat.V.00.01.fits'
    client_mock.data_client.info.side_effect = lambda x: FileInfo(id=x, md5sum='abc')
    test_config.change_working_directory(tmp_path)
    # test_config.logging_level = 'DEBUG'
    test_config.task_types = [mc.TaskType.STORE, mc.TaskType.INGEST]
    test_config.use_local_files = True
    test_data_source = '/data/imaginary'
    test_config.data_sources = [test_data_source]
    test_config.interval = 1200
    test_config.retry_failures = True
    test_config.retry_decay = 0
    test_config.retry_count = 2
    mc.State.write_bookmark(test_config.state_fqn, test_config.bookmark, datetime(2019, 4, 29, 12, 34))
    client_mock.metadata_client.read.return_value = None
    orig_getcwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        mc.Config.write_to_file(test_config)
        test_metadata_reader = Hdf5FileMetadataReader()
        test_source = dsc.LocalFilesDataSource(test_config, client_mock.data_client, test_metadata_reader)
        # mock _append_work because want to test the _capture_todo call in get_time_box_work
        test_source._temp = defaultdict(list)
        test_source._temp[datetime(2019, 4, 30, 1, 1, 1)].append(f'{test_data_source}/{test_f_1}')
        test_source._temp[datetime(2019, 4, 30, 2, 2, 2)].append(f'{test_data_source}/{test_f_2}')
        test_source._temp[datetime(2019, 5, 2)].append(f'{test_data_source}/{test_f_3}')
        test_source._rejected_files = 1
        test_source._skipped_files = 1
        test_sources = [test_source]
        test_name_builder = nbc.GuessingBuilder(mc.StorageName)
        test_result = rc.run_by_state(
            config=test_config,
            meta_visitors=[visit_mock],
            sources=test_sources,
            metadata_reader=test_metadata_reader,
            clients=client_mock,
            name_builder=test_name_builder,
        )
        assert test_result is not None, 'expect result'
        assert test_result == -1, 'expect failures because of the retries'
        assert client_mock.metadata_client.read.called, 'read called'
        # 6 = 1 success + 2 failures + 1 retry success + 1 retry failure + 1 second retry failure
        assert client_mock.metadata_client.read.call_count == 6, 'read call count'
        assert client_mock.metadata_client.create.called, 'create called'
        assert client_mock.metadata_client.create.call_count == 6, 'create call count'
        assert client_mock.data_client.put.called, 'put should be called'
        assert client_mock.data_client.put.call_count == 6, 'wrong number of puts'
        client_mock.data_client.put.assert_called_with(
            test_data_source, f'{test_config.scheme}:{test_config.collection}/{test_f_3}'
        )
        assert not client_mock.data_client.get_head.called, 'get_head not called, not a DelayedClientReader'
        assert not client_mock.data_client.info.called, 'info called, not a DelayedClientReader'
        test_reporter = test_sources[0].reporter
        assert test_reporter._summary.entries == 5, 'all'
        assert test_reporter._summary._errors_sum == 4, 'errors'
        assert test_reporter._summary._retry_sum == 3, 'retry'
        assert test_reporter._summary.success == 2, 'success'
        assert test_reporter._summary._timeouts_sum == 0, 'timeouts'
        assert test_reporter._summary._rejected_sum == 1, 'rejected'
        assert test_reporter._summary._skipped_sum == 1, 'skipped'
    finally:
        os.chdir(orig_getcwd)


def _write_todo(test_config):
    with open(test_config.work_fqn, 'w') as f:
        f.write(f'test_obs_id.fits.gz')


call_count = 0


def _mock_get_compressed_file(working_dir, uri):
    fqn = f'{working_dir}/{os.path.basename(uri)}'
    with open(fqn, 'wb') as f:
        f.write(
            b"\x1f\x8b\x08\x08\xd0{Lb\x02\xff.abc.fits\x00+I-.QH\xce"
            b"\xcf+I\xcd+\xe1\x02\x00\xbd\xdfZ'\r\x00\x00\x00"
        )


def _mock_get_work(arg1, arg2):
    return _mock_query(None, None, None)


def _mock_query_table2(arg1, arg2):
    return _mock_query_table(arg1, None, None)


def _mock_query_table(arg1, arg2, arg3):
    global call_count
    if 'max(' in arg1:
        return Table.read('m\n2020-01-23T00:53:58.000\n'.split('\n'), format='csv')
    else:
        if call_count == 0:
            call_count = 1
            return Table.read(
                'uri,lastModified\n'
                'cadc:NEOSSAT/NEOS_SCI_2015347000000_clean.fits,'
                '2019-10-23T16:27:19.000\n'.split('\n'),
                format='csv',
            )
        else:
            return Table.read('fileName,ingestDate\n'.split('\n'), format='csv')


def _mock_query(arg1, arg2, arg3):
    temp = deque()
    global call_count
    if call_count == 0:
        call_count = 1
        temp.append(
            dsc.StateRunnerMeta(
                'NEOS_SCI_2015347000000_clean.fits',
                datetime.strptime('2019-10-23T16:27:19.000', '%Y-%m-%dT%H:%M:%S.%f'),
            )
        )
        return temp
    return temp


def _mock_query2(arg1, arg2):
    return _mock_query(None, None, None)


def _mock_do_one_failure(arg1):
    assert isinstance(arg1, mc.StorageName), 'expect StorageName instance'
    if arg1.obs_id == 'TEST_OBS_ID':
        assert arg1.file_name == 'TEST_OBS_ID.fits', 'wrong file name'
        with open(f'{tc.TEST_DATA_DIR}/retry.txt', 'w') as f:
            f.write(f'ghi.fits.gz')
    elif arg1.obs_id == 'ghi':
        assert arg1.file_name == 'ghi.fits', 'wrong file name'
    elif arg1.obs_id == 'NEOS_SCI_2015347000000_clean':
        # TODO - why is this required now?
        pass
    else:
        assert False, f'unexpected obs id {arg1.obs_id}'
    # mock execution failure
    return -1, None


def _mock_write():
    fqn = (
        f'{tc.THIS_DIR}/NEOS_SCI_2015347000000_clean/'
        f'NEOS_SCI_2015347000000_clean.xml'
    )
    mc.write_obs_to_file(
        SimpleObservation(
            collection='test_collection',
            observation_id='ghi',
            algorithm=Algorithm('test'),
        ),
        fqn,
    )


def _mock_read2(ign1, ign2):
    return _mock_read(None)


def _mock_read(ignore_fqn):
    return SimpleObservation(
        collection='test_collection',
        observation_id='ghi',
        algorithm=Algorithm('test'),
    )


def _mock_visit():
    return SimpleObservation(
        collection='test_collection',
        observation_id='ghi',
        algorithm=Algorithm('test'),
    )
