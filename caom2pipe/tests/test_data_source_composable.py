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

import os
import pytest
import shutil

from astropy.table import Table
from cadctap import CadcTapClient
from cadcutils import exceptions
from datetime import datetime, timedelta, timezone
from pathlib import Path
from time import sleep
from unittest.mock import Mock, patch, call

from cadcdata import FileInfo
from caom2pipe import data_source_composable as dsc
from caom2pipe import manage_composable as mc
from caom2pipe import reader_composable as rdc

import test_conf as tc


def test_list_dir_data_source(test_config, tmpdir):
    test_config.change_working_directory(tmpdir)

    for entry in [
        'TEST.fits.gz',
        'TEST1.fits',
        'TEST2.fits.fz',
        'TEST3.hdf5',
    ]:
        with open(f'{test_config.working_directory}/{entry}', 'w') as f:
            f.write('test content')

    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_chooser = tc.TChooser()
    test_subject = dsc.ListDirDataSource(test_config, test_chooser)
    test_subject.reporter = test_reporter
    test_result = test_subject.get_work()
    assert test_result is not None, 'expected a result'
    assert len(test_result) == 4, 'wrong result'
    assert 'TEST.fits.gz' in test_result, 'wrong gz extensions'
    assert 'TEST1.fits.gz' in test_result, 'wrong no extension'
    assert 'TEST2.fits.fz' in test_result, 'wrong fz extensions'
    assert 'TEST3.hdf5' in test_result, 'wrong hdf5'
    assert test_reporter.all == 4, 'wrong report'

    test_subject = dsc.ListDirDataSource(test_config, chooser=None)
    test_subject.reporter = test_reporter
    test_result = test_subject.get_work()
    assert test_result is not None, 'expected a result'
    assert len(test_result) == 4, 'wrong result'
    assert 'TEST.fits.gz' in test_result, 'wrong gz extensions'
    assert 'TEST1.fits' in test_result, 'wrong no extension'
    assert 'TEST2.fits.fz' in test_result, 'wrong fz extensions'
    assert 'TEST3.hdf5' in test_result, 'wrong hdf5'
    assert test_reporter.all == 8, 'wrong 2nd report'


def test_todo_file(test_config, tmpdir):
    todo_fqn = os.path.join(tmpdir, 'todo.txt')
    with open(todo_fqn, 'w') as f:
        f.write('file1\n')
        f.write('file2\n')
        f.write('\n')

    test_config.work_fqn = todo_fqn
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject = dsc.TodoFileDataSource(test_config)
    test_subject.reporter = test_reporter
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect result'
    assert len(test_result) == 2, 'wrong number of files'
    assert test_reporter.all == 2, 'wrong report'


@patch('caom2pipe.client_composable.query_tap_client')
def test_storage_time_box_query(query_mock, test_config, tmpdir):
    def _mock_query(arg1, arg2):
        return Table.read(
            'uri,lastModified\n'
            'cadc:NEOSSAT/NEOS_SCI_2015347000000_clean.fits,2019-10-23T16:27:19.000\n'
            'cadc:NEOSSAT/NEOS_SCI_2015347000000.fits,2019-10-23T16:27:27.000\n'
            'cadc:NEOSSAT/NEOS_SCI_2015347002200_clean.fits,2019-10-23T16:27:33.000\n'.split(
                '\n'
            ),
            format='csv',
        )

    query_mock.side_effect = _mock_query
    tap_client_ctor_orig = CadcTapClient.__init__
    CadcTapClient.__init__ = Mock(return_value=None)
    utc_now = datetime.now(tz=timezone.utc)
    prev_exec_date = utc_now - timedelta(seconds=3600)
    exec_date = utc_now - timedelta(seconds=1800)
    try:
        test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
        test_subject = dsc.QueryTimeBoxDataSource(test_config)
        test_subject.reporter = test_reporter
        test_result = test_subject.get_time_box_work(prev_exec_date, exec_date)
        assert test_result is not None, 'expect result'
        assert len(test_result) == 3, 'wrong number of results'
        assert (
            test_result[0].entry_name == 'NEOS_SCI_2015347000000_clean.fits'
        ), 'wrong results'
        assert test_reporter.all == 3, 'wrong report'
    finally:
        CadcTapClient.__init__ = tap_client_ctor_orig


def test_vault_list_dir_data_source(test_config, tmpdir):
    def _query_mock(ignore_source_directory):
        return ['abc.txt', 'abc.fits', 'def.fits', '900898p_moc.fits']

    node1 = type('', (), {})()
    node1.props = {
        'size': 0,
    }
    node1.uri = 'vos://cadc.nrc.ca!vault/goliaths/wrong/900898p_moc.fits'
    node1.type = 'vos:DataNode'
    node2 = type('', (), {})()
    node2.props = {
        'size': 12,
    }
    node2.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/abc.fits'
    node2.type = 'vos:DataNode'
    node3 = type('', (), {})()
    node3.props = {
        'size': 12,
    }
    node3.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/abc.txt'
    node3.type = 'vos:DataNode'
    node4 = type('', (), {})()
    node4.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc'
    node4.type = 'vos:ContainerNode'
    node5 = type('', (), {})()
    node5.props = {
        'size': 12,
    }
    node5.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/def.fits'
    node5.type = 'vos:DataNode'
    node4.node_list = [node1, node2, node3, node5]

    test_vos_client = Mock()
    test_vos_client.listdir.side_effect = _query_mock
    test_vos_client.get_node.side_effect = [node4, node1, node2, node3, node5]
    test_config.change_working_directory(tmpdir)
    test_config.data_sources = ['vos:goliaths/wrong']
    test_config.data_source_extensions = ['.fits']
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject = dsc.VaultDataSource(test_vos_client, test_config)
    test_subject.reporter = test_reporter
    assert test_subject is not None, 'expect a test_subject'
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a test result'
    assert len(test_result) == 2, 'wrong number of results'
    test_entry = test_result.popleft()
    assert 'vos://cadc.nrc.ca!vault/goliaths/moc/abc.fits' == test_entry, 'wrong result'
    test_entry = test_result.popleft()
    assert 'vos://cadc.nrc.ca!vault/goliaths/moc/def.fits' == test_entry, 'wrong result'
    assert test_reporter.all == 2, 'wrong report'


def test_list_dir_time_box_data_source(test_config, tmpdir):
    test_prev_exec_time_dt = datetime.now()

    sleep(1)
    test_sub_dir = f'{tmpdir}/sub_directory'
    test_file_1 = os.path.join(tmpdir, 'abc1.fits')
    test_file_2 = os.path.join(test_sub_dir, 'abc2.fits')

    # so the timestamps are correct, delete then re-create the test directory structure
    os.mkdir(test_sub_dir)

    for entry in [test_file_1, test_file_2]:
        sleep(1)
        with open(entry, 'w') as f:
            f.write('test content')

    test_config.working_directory = tc.TEST_DATA_DIR
    test_config.data_sources = [tmpdir]
    test_config.data_source_extensions = ['.fits']
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_exec_time_dt = test_prev_exec_time_dt + timedelta(seconds=3600.0)

    test_subject = dsc.ListDirTimeBoxDataSource(test_config)
    assert test_subject is not None, 'ctor is broken'
    test_subject.reporter = test_reporter
    test_result = test_subject.get_time_box_work(test_prev_exec_time_dt, test_exec_time_dt)
    assert test_result is not None, 'expect a result'
    assert len(test_result) == 2, 'expect contents in the result'
    test_entry = test_result.popleft()
    assert (
        test_entry.entry_name == test_file_1
    ), 'wrong expected file, order matters since should be sorted deque'
    assert test_reporter.all == 2, 'wrong report'

    test_config.recurse_data_sources = False
    test_subject = dsc.ListDirTimeBoxDataSource(test_config)
    test_subject.reporter = test_reporter
    test_result = test_subject.get_time_box_work(test_prev_exec_time_dt, test_exec_time_dt)
    assert test_result is not None, 'expect a non-recursive result'
    assert len(test_result) == 1, 'expect contents in non-recursive result'
    x = [ii.entry_name for ii in test_result]
    assert test_file_2 not in x, 'recursive result should not be present'
    assert test_reporter.all == 3, 'wrong 2nd report'


def test_list_dir_separate_data_source(test_config):
    test_config.data_sources = ['/test_files']
    test_config.data_source_extensions = [
        '.fits',
        '.fits.gz',
        '.fits.fz',
        '.hdf5',
    ]
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject = dsc.ListDirSeparateDataSource(test_config)
    test_subject.reporter = test_reporter
    assert test_subject is not None, 'ctor is broken'
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a result'
    assert len(test_result) == 106, 'expect contents in the result'
    assert '/test_files/sub_directory/abc.fits' in test_result, 'wrong entry'
    assert test_reporter.all == 106, 'wrong report'

    test_config.recurse_data_sources = False
    test_subject = dsc.ListDirSeparateDataSource(test_config)
    test_subject.reporter = test_reporter
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a non-recursive result'
    assert len(test_result) == 101, 'expect contents in non-recursive result'
    assert (
        '/test_files/sub_directory/abc.fits' not in test_result
    ), 'recursive result should not be present'
    assert test_reporter.all == 207, 'wrong 2nd report'


def test_vault_list_dir_time_box_data_source(test_config):
    node_listing = _create_vault_listing()
    test_vos_client = Mock()
    test_vos_client.get_node.side_effect = node_listing
    test_config.data_sources = ['vos:goliaths/wrong']
    test_config.data_source_extensions = ['.fits']
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject = dsc.VaultDataSource(test_vos_client, test_config)
    assert test_subject is not None, 'expect a test_subject'
    test_subject.reporter = test_reporter
    test_prev_exec_dt = datetime(year=2020, month=9, day=15, hour=10, minute=0, second=0)
    test_exec_dt = datetime(year=2020, month=9, day=16, hour=10, minute=0, second=0)
    test_result = test_subject.get_time_box_work(test_prev_exec_dt, test_exec_dt)
    assert test_result is not None, 'expect a test result'
    assert len(test_result) == 1, 'wrong number of results'
    assert (
        'vos://cadc.nrc.ca!vault/goliaths/moc/994898p_moc.fits'
        == test_result[0].entry_name
    ), 'wrong name result'
    # the datetime from the record in the test_result, which should be between the start and stop times
    assert (
        datetime(year=2020, month=9, day=15, hour=19, minute=55, second=3, microsecond=67000)
        == test_result[0].entry_dt
    ), 'wrong dt result'
    assert test_reporter.all == 1, 'wrong report'
    assert test_vos_client.get_node.called, 'get_node call'
    assert test_vos_client.get_node.call_count == 3, 'get_node call count'
    test_vos_client.get_node.assert_has_calls(
        [
            call('vos:goliaths/wrong', limit=None, force=True),
            call('vos://cadc.nrc.ca!vault/goliaths/moc/994898p_moc.fits'),
            call('vos://cadc.nrc.ca!vault/goliaths/moc/994899p_moc.fits')
        ]
    )


def test_transfer_check_fits_verify(test_config, tmpdir):
    # how things should probably work at CFHT
    delta = timedelta(minutes=30)
    # half an hour ago
    test_start_time = datetime.now() - delta
    # half an hour from now
    test_end_time = test_start_time + delta + delta
    test_source_directory = Path(f'{tmpdir}/cfht_source')
    test_failure_directory = Path(f'{tmpdir}/cfht_transfer_failure')
    test_success_directory = Path(f'{tmpdir}/cfht_transfer_success')
    test_empty_file = Path(f'{tmpdir}/cfht_source/empty_file.fits.fz')
    test_broken_file = Path(f'{tmpdir}/cfht_source/broken.fits')
    test_correct_file = Path(f'{tmpdir}/cfht_source/correct.fits.gz')
    test_dot_file = Path(f'{tmpdir}/cfht_source/.dot_file.fits')
    test_same_file = Path(f'{tmpdir}/cfht_source/same_file.fits')
    test_broken_source = Path('/test_files/broken.fits')
    test_correct_source = Path('/test_files/correct.fits.gz')
    test_same_source = Path('/test_files/same_file.fits')
    test_already_successful_source = Path('/test_files/already_successful.fits')
    test_reader = rdc.FileMetadataReader()

    def mock_info(uri):
        return FileInfo(
            id=uri,
            size=12,
            md5sum='e4e153121805745792991935e04de322',
        )

    def _at_cfht(test_start_ts, test_end_ts):
        test_config.cleanup_files_when_storing = True
        test_config.task_types = [mc.TaskType.STORE]
        test_config.retry_failures = False
        cadc_client_mock = Mock(autospec=True)
        cadc_client_mock.info.side_effect = mock_info
        test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
        test_subject = dsc.LocalFilesDataSource(test_config, cadc_client_mock, test_reader)
        test_subject.reporter = test_reporter

        assert test_subject is not None, 'expect construction to work'
        test_result = test_subject.get_time_box_work(test_start_ts, test_end_ts)
        assert len(test_result) == 1, 'wrong number of results returned'
        assert test_result[0].entry_name == f'{tmpdir}/cfht_source/correct.fits.gz', 'wrong result'

        for e in [test_empty_file, test_broken_file]:
            # both should fail the ac.check_fits call
            assert not e.exists(), f'file at source {e}'
            moved = Path(test_failure_directory, e.name)
            assert moved.exists(), f'file at destination {e}'

        # same file has been moved
        assert not test_same_file.exists(), 'same file at source'
        moved = Path(test_success_directory, test_same_file.name)
        assert moved.exists(), 'same file at destination'

        # correct file - the file stays where it is until it's transferred
        assert test_correct_file.exists(), 'correct file at source'
        moved_success = Path(test_success_directory, test_correct_file.name)
        moved_failure = Path(test_failure_directory, test_correct_file.name)
        assert not moved_success.exists(), 'correct file at success'
        assert not moved_failure.exists(), 'correct file at failure'
        assert test_reporter.all == 5, f'wrong all {test_reporter._summary}'
        assert test_reporter._summary._skipped_sum == 2, f'wrong skipped {test_reporter._summary}'
        assert test_reporter._summary._rejected_sum == 2, f'wrong rejected {test_reporter._summary}'

        # and after the transfer
        for test_entry in test_result:
            # execution result == -1, execution failed, so delay clean-up
            test_subject.clean_up(test_entry, -1, current_count=0)
        assert not test_correct_file.exists(), 'correct file at source'
        assert not moved_success.exists(), 'correct file at destination'
        assert moved_failure.exists(), 'correct file at destination'

        # dot file - which should be ignored
        assert test_dot_file.exists(), 'correct file at source'
        moved = Path(test_success_directory, test_dot_file.name)
        assert not moved.exists(), 'dot file at success destination'
        moved = Path(test_failure_directory, test_dot_file.name)
        assert not moved.exists(), 'dot file at failure destination'
        assert test_reporter.all == 5, f'wrong all {test_reporter._summary}'
        assert test_reporter._summary._skipped_sum == 2, f'wrong skipped {test_reporter._summary}'
        assert test_reporter._summary._rejected_sum == 2, f'wrong rejected {test_reporter._summary}'

    def _at_cadc(test_start_ts, test_end_ts):
        test_config.cleanup_files_when_storing = False
        cadc_client_mock = Mock(autospec=True)
        cadc_client_mock.info.side_effect = mock_info
        test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
        test_subject = dsc.LocalFilesDataSource(test_config, cadc_client_mock, test_reader)
        test_subject.reporter = test_reporter
        assert test_subject is not None, 'expect construction to work'
        test_result = test_subject.get_time_box_work(test_start_ts, test_end_ts)
        assert len(test_result) == 3, 'wrong number of results returned'
        assert test_result[0].entry_name == f'{tmpdir}/cfht_source/correct.fits.gz', 'wrong result'
        assert test_result[1].entry_name == f'{tmpdir}/cfht_source/same_file.fits', 'wrong result'
        assert (
            test_result[2].entry_name == f'{tmpdir}/cfht_source/already_successful.fits'
        ), 'wrong result'
        for f in [test_empty_file, test_broken_file, test_correct_file, test_dot_file, test_same_file]:
            assert f.exists(), 'file at source'
            moved = Path(test_failure_directory, f.name)
            assert not moved.exists(), 'file at destination'
        # clean up should do nothing
        for test_entry in test_result:
            # execution result == -1, execution failed, so delay clean-up
            test_subject.clean_up(test_entry, -1, current_count=0)
        for f in [test_empty_file, test_broken_file, test_correct_file, test_dot_file, test_same_file]:
            assert f.exists(), 'file at source'
            moved = Path(test_failure_directory, f.name)
            assert not moved.exists(), 'file at destination'
        assert test_reporter.all == 5, 'wrong report'
        assert test_reporter._summary._skipped_sum == 0, f'wrong skipped {test_reporter._summary}'
        assert test_reporter._summary._rejected_sum == 2, f'wrong rejected {test_reporter._summary}'

    def _move_failure(test_start_ts, test_end_ts):
        def _move_mock():
            raise mc.CadcException('move mock')

        move_orig = shutil.move
        shutil.move = Mock(side_effect=_move_mock)
        try:
            test_config.cleanup_files_when_storing = True
            test_config.task_types = [mc.TaskType.STORE]
            cadc_client_mock = Mock(autospec=True)
            cadc_client_mock.info.side_effect = mock_info
            test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
            test_subject = dsc.LocalFilesDataSource(test_config, cadc_client_mock, test_reader)
            test_subject.reporter = test_reporter
            with pytest.raises(mc.CadcException):
                test_result = test_subject.get_time_box_work(test_start_ts, test_end_ts)
            assert test_reporter.all == 0, 'wrong report'
            assert test_reporter._summary._skipped_sum == 0, f'wrong skipped {test_reporter._summary}'
            assert test_reporter._summary._rejected_sum == 0, f'wrong rejected {test_reporter._summary}'
        finally:
            shutil.move = move_orig

    for test in [_at_cfht, _at_cadc, _move_failure]:
        for entry in [test_failure_directory, test_success_directory, test_source_directory]:
            if not entry.exists():
                entry.mkdir()
            for child in entry.iterdir():
                child.unlink()
        for entry in [test_empty_file, test_dot_file]:
            entry.touch()
        for source in [
            test_broken_source, test_correct_source, test_same_source, test_already_successful_source
        ]:
            shutil.copy(source, test_source_directory)

        # CFHT test case - try to move a file that would have the effect
        # of replacing a file already in the destination directory
        shutil.copy(test_already_successful_source, test_success_directory)

        test_config.use_local_files = True
        test_config.data_sources = [test_source_directory.as_posix()]
        test_config.data_source_extensions = ['.fits', '.fits.gz', '.fits.fz']
        test_config.cleanup_success_destination = test_success_directory.as_posix()
        test_config.cleanup_failure_destination = test_failure_directory.as_posix()
        test_config.store_modified_files_only = True

        test(test_start_time, test_end_time)


@patch('caom2pipe.astro_composable.check_fits')
def test_transfer_fails(check_fits_mock, test_config, tmpdir):
    check_fits_mock.return_value = True
    # set up a correct transfer
    test_source_directory = Path(f'{tmpdir}/cfht_source')
    test_failure_directory = Path(f'{tmpdir}/cfht_transfer_failure')
    test_success_directory = Path(f'{tmpdir}/cfht_transfer_success')
    test_correct_file_1 = Path(f'{tmpdir}/cfht_source/correct_1.fits.gz')
    test_correct_file_2 = Path(f'{tmpdir}/cfht_source/correct_2.fits.gz')
    test_correct_source = Path('/test_files/correct.fits.gz')
    test_reader = rdc.FileMetadataReader()

    for entry in [test_failure_directory, test_success_directory, test_source_directory]:
        entry.mkdir()

    shutil.copy(test_correct_source, test_correct_file_1)
    shutil.copy(test_correct_source, test_correct_file_2)

    test_config.data_sources = [test_source_directory.as_posix()]
    test_config.task_types = [mc.TaskType.STORE]
    test_config.data_source_extensions = ['.fits.gz']
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = test_failure_directory.as_posix()
    test_config.cleanup_success_destination = test_success_directory.as_posix()

    cadc_client_mock = Mock(autospec=True)
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject = dsc.LocalFilesDataSource(test_config, cadc_client_mock, test_reader)
    assert test_subject is not None, 'ctor failure'
    test_subject.reporter = test_reporter
    test_result = test_subject.get_work()
    assert test_result is not None, 'expect a result'
    assert len(test_result) == 2, 'wrong number of entries in result'

    match = FileInfo(id=test_correct_file_1.as_posix(), size=12, md5sum='dfb5ddab74844d911dd54552415dd8ab')
    different = FileInfo(id=test_correct_file_2.as_posix(), size=12, md5sum='e4e153121805745792991935e04de322')

    cadc_client_mock.info.side_effect = [match, different]
    for test_entry in test_result:
        # execution result == -1, execution failed, so delay clean-up
        test_subject.clean_up(test_entry, -1, current_count=0)

    assert not test_correct_file_1.exists(), 'file 1 should be moved'
    assert not test_correct_file_2.exists(), 'file 2 should be moved'
    moved_1 = Path(test_success_directory, test_correct_file_1.name)
    moved_2 = Path(test_failure_directory, test_correct_file_2.name)
    assert moved_1.exists(), 'file 1 not moved to correct location'
    assert moved_2.exists(), 'file 2 not moved to correct location'
    # no work done yet
    assert test_reporter.all == 2, 'wrong report'
    assert test_reporter._summary._success_sum == 0, f'wrong report {test_reporter._summary}'
    assert test_reporter._summary._rejected_sum == 0, f'wrong report {test_reporter._summary}'


@patch('caom2pipe.data_source_composable.LocalFilesDataSource._verify_file')
@patch('caom2pipe.data_source_composable.LocalFilesDataSource._move_action')
@patch('caom2pipe.client_composable.ClientCollection')
def test_all_local_files_some_already_stored_some_broken(clients_mock, move_mock, verify_mock, test_config):
    # work is all the list of files, some of which have been stored already, and some of which fail fitsverify

    test_config.task_types = [mc.TaskType.STORE]
    test_config.use_local_files = True
    test_config.data_sources = ['/tmp']
    test_config.cleanup_files_when_storing = True
    test_config.cleanup_failure_destination = '/data/failure'
    test_config.cleanup_success_destination = '/data/success'
    test_config.store_modified_files_only = True

    pre_success_listing, file_info_list = _create_dir_listing('/tmp', 3)
    test_reader = Mock()
    test_reader.file_info.get.side_effect = [file_info_list[0], None, None]
    clients_mock.return_value.data_client.info.side_effect = [file_info_list[0], None, None]
    verify_mock.side_effect = [True, False, True]

    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject = dsc.LocalFilesDataSource(
        test_config,
        clients_mock.return_value.data_client,
        test_reader,
        recursive=True,
        scheme='cadc',
    )
    test_subject.reporter = test_reporter

    with patch('os.scandir') as scandir_mock:
        scandir_mock.return_value.__enter__.return_value = pre_success_listing
        test_result = test_subject.get_work()
        assert test_result is not None, 'expect a result'
        assert len(test_result) == 1, 'wrong number of results'
        assert test_reporter.all == 3, 'wrong report'
        assert test_reporter._summary._skipped_sum == 1, f'wrong report {test_reporter._summary}'
        assert test_reporter._summary._rejected_sum == 1, f'wrong report {test_reporter._summary}'
        assert move_mock.called, 'move should be called'
        assert move_mock.call_count == 2, 'wrong move call count'
        move_mock.assert_has_calls([call('/tmp/A0.fits', '/data/success'), call('/tmp/A1.fits', '/data/failure')]), 'wrong move_mock calls'


@patch('caom2pipe.client_composable.vault_info', autospec=True)
def test_vo_transfer_check_fits_verify(vault_info_mock, test_config):
    test_match_file_info = FileInfo(id='vos:abc/def.fits', md5sum='ghi')
    test_different_file_info = FileInfo(id='vos:abc/def.fits', md5sum='ghi')
    test_file_info = [test_match_file_info, test_different_file_info]

    test_data_client = Mock(autospec=True)
    test_vos_client = Mock(autospec=True)

    test_config.data_source_extensions = ['.fits.gz']
    test_config.data_sources = ['vos:DAO/Archive/Incoming']
    test_config.cleanup_failure_destination = 'vos:DAO/failure'
    test_config.cleanup_success_destination = 'vos:DAO/success'
    test_config.recurse_data_sources = False

    def _mock_listdir(entry):
        if entry.endswith('Incoming'):
            return ['dao123.fits.gz', 'dao456.fits', 'Yesterday', '.dot.fits.gz']
        else:
            return []

    test_vos_client.listdir.side_effect = _mock_listdir
    test_vos_client.isdir.side_effect = [False, False, True, False, False, False, True, False]
    test_reader = rdc.VaultReader(test_vos_client)

    for case in [True, False]:
        test_config.cleanup_files_when_storing = case
        vault_info_mock.side_effect = test_file_info
        test_data_client.info.side_effect = test_file_info

        test_subject = dsc.VaultCleanupDataSource(test_config, test_vos_client, test_data_client, test_reader)
        assert test_subject is not None, 'expect ctor to work'
        test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
        test_subject.reporter = test_reporter
        test_result = test_subject.get_work()

        assert test_result is not None, 'expect a work list'
        assert len(test_result) == 1, 'wrong work list entries'
        assert test_result[0] == 'vos:DAO/Archive/Incoming/dao123.fits.gz', 'wrong work entry'

        assert test_vos_client.isdir.call_count == 4, 'wrong is_dir count'
        test_vos_client.isdir.reset_mock()
        # no work done yet
        assert test_reporter.all == 1, 'wrong report'
        assert test_reporter._summary._success_sum == 0, f'wrong report {test_reporter._summary}'
        assert test_reporter._summary._rejected_sum == 0, f'wrong report {test_reporter._summary}'
        assert test_reporter._summary._skipped_sum == 0, f'wrong report {test_reporter._summary}'

    # test the case when the md5sums are the same, so the transfer does
    # not occur, but the file ends up in the success location
    test_vos_client.isdir.side_effect = [False, False, True, False]
    test_config.cleanup_files_when_storing = True
    test_config.store_modified_files_only = True
    test_config.task_types = [mc.TaskType.STORE]
    vault_info_mock.return_value = test_match_file_info
    test_data_client.info.return_value = test_match_file_info
    test_vos_client.status.raises = exceptions.NotFoundException

    second_test_subject = dsc.VaultCleanupDataSource(test_config, test_vos_client, test_data_client, test_reader)
    assert second_test_subject is not None, 'second ctor fails'
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    second_test_subject.reporter = test_reporter
    second_test_result = second_test_subject.get_work()
    assert second_test_result is not None, 'expect a second result'
    assert len(second_test_result) == 0, 'should be no successes'
    assert test_vos_client.move.called, 'expect a success move call'
    test_vos_client.move.assert_called_with(
        'vos:DAO/Archive/Incoming/dao123.fits.gz', 'vos:DAO/success/dao123.fits.gz'
    )
    assert test_vos_client.status.called, 'expect a status call'
    assert test_reporter.all == 1, 'wrong report'
    assert test_reporter._summary._success_sum == 1, f'wrong report {test_reporter._summary}'
    assert test_reporter._summary._rejected_sum == 0, f'wrong report {test_reporter._summary}'
    assert test_reporter._summary._skipped_sum == 1, f'wrong report {test_reporter._summary}'


def test_vault_clean_up_get_time_box(test_config):
    test_match_file_info = FileInfo(id='vos:abc/def.fits', md5sum='ghi')
    test_different_file_info = FileInfo(id='vos:abc/def.fits', md5sum='ghi')
    test_file_info = [test_match_file_info, test_different_file_info]

    test_data_client = Mock(autospec=True)

    test_config.data_source_extensions = ['.fits']
    test_config.data_sources = ['vos:DAO/Archive/Incoming']
    test_config.cleanup_failure_destination = 'vos:DAO/failure'
    test_config.cleanup_success_destination = 'vos:DAO/success'
    test_config.store_modified_files_only = True
    test_config.recurse_data_sources = False

    node_listing = _create_vault_listing()
    test_vos_client = Mock(autospec=True)

    for cleanup_files in [True, False]:
        # get_node is called twice for each DataNode, once originally, and a second time by the MetadataReader
        test_vos_client.get_node.side_effect = [
            node_listing[0], node_listing[1], node_listing[1], node_listing[2], node_listing[2]
        ]
        test_reader = rdc.VaultReader(test_vos_client)
        test_config.cleanup_files_when_storing = cleanup_files
        test_data_client.info.side_effect = test_file_info

        test_subject = dsc.VaultCleanupDataSource(test_config, test_vos_client, test_data_client, test_reader)
        assert test_subject is not None, 'expect ctor to work'
        test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
        test_subject.reporter = test_reporter
        test_prev_exec_time = datetime(year=2020, month=9, day=15, hour=10, minute=0, second=0)
        test_exec_time = datetime(year=2020, month=9, day=16, hour=10, minute=0, second=0)
        test_result = test_subject.get_time_box_work(test_prev_exec_time, test_exec_time)

        assert test_result is not None, 'expect a work list'
        assert len(test_result) == 1, 'wrong work list entries'
        assert test_result[0].entry_name == 'vos://cadc.nrc.ca!vault/goliaths/moc/994898p_moc.fits', 'wrong work entry url'
        assert (
            test_result[0].entry_dt == datetime(2020, 9, 15, 19, 55, 3, 67000)
        ), 'wrong work entry timestamp on file modification'

        assert test_vos_client.isdir.call_count == 0, 'wrong is_dir count'
        # skip count should be 0, because the time-box should exclude 994899p_moc.fits
        assert test_reporter.all == 1, f'wrong report {test_reporter._summary}'
        assert test_reporter._summary._success_sum == 0, f'wrong success  {test_reporter._summary}'
        assert test_reporter._summary._rejected_sum == 0, f'wrong rejected {test_reporter._summary}'
        assert test_reporter._summary._skipped_sum == 0, f'wrong skipped {test_reporter._summary}'
        assert test_reporter._summary._errors_sum == 0, f'wrong errors {test_reporter._summary}'
        # no work done yet, no clean ups called
        assert not test_vos_client.move.called, 'move mock'


def test_data_source_exists(test_config):
    # test the case where the destination file already exists, so the
    # move cleanup has to remove it first
    test_uri = 'cadc:OMM/dest_fqn.fits'
    test_config.cleanup_failure_destination = 'vos:test/failure'
    test_config.cleanup_success_destination = 'vos:test/success'
    test_config.data_sources = 'vos:test'
    test_config.data_source_extensions = ['.fits']
    test_config.cleanup_files_when_storing = True
    test_config.task_types = [mc.TaskType.STORE]
    test_vos_client = Mock(autospec=True)
    test_data_client = Mock(autospec=True)
    test_reader = rdc.VaultReader(test_vos_client)
    test_reader.file_info[test_uri] = FileInfo(id=test_uri, md5sum='ghi')
    test_subject = dsc.VaultCleanupDataSource(test_config, test_vos_client, test_data_client, test_reader)
    assert test_subject is not None, 'ctor failure'
    test_reporter = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    test_subject.reporter = test_reporter
    test_subject._work = ['vos:test/dest_fqn.fits']

    def _get_node(uri, limit=None, force=None):
        assert uri == 'vos:test/dest_fqn.fits', f'wrong vo check {uri}'
        node = type('', (), {})()
        node.props = {'length': 42, 'MD5': 'ghi', 'lastmod': 'Sept 10 2021'}
        return node

    test_vos_client.get_node.side_effect = _get_node

    # mock that the same file already exists as CADC
    def _get_info(uri):
        assert uri == test_uri, f'wrong storage check {uri}'
        return FileInfo(id=uri, md5sum='ghi')

    test_data_client.info.side_effect = _get_info

    # destination file exists at CADC
    def _status(uri):
        assert uri == 'vos:test/success/dest_fqn.fits', f'wrong status check {uri}'
        return True

    test_vos_client.status.side_effect = _status

    # test execution
    for entry in test_subject._work:
        test_subject.clean_up(entry, 'ignore1', 'ignore2')
    assert test_vos_client.status.called, 'expect status call'
    assert test_vos_client.delete.called, 'expect delete call'
    test_vos_client.delete.assert_called_with('vos:test/success/dest_fqn.fits')
    assert test_vos_client.move.called, 'expect move call'
    test_vos_client.move.assert_called_with('vos:test/dest_fqn.fits', 'vos:test/success/dest_fqn.fits')


def _create_dir_listing(root_dir, count, prefix='A'):
    stat_return_value = type('', (), {})
    stat_return_value.st_size = 123

    listing_result = []
    file_info_list = []
    for ii in range(0, count):
        dir_entry = type('', (), {})
        dir_entry.name = f'{prefix}{ii}.fits'
        dir_entry.path = f'{root_dir}/{dir_entry.name}'
        stat_return_value.st_mtime = 1583197266.0 + 5.0 * ii
        dir_entry.stat = Mock(return_value=stat_return_value)
        dir_entry.is_dir = Mock(return_value=False)
        listing_result.append(dir_entry)
        file_info_list.append(FileInfo(id=dir_entry.name, size=123, md5sum='md5:abc'))
    return listing_result, file_info_list


def _create_vault_listing():
    node1 = type('', (), {})()
    node1.props = {
        'date': '2020-09-15 19:55:03.067000+00:00',
        'size': 14,
        'MD5': 'def',
    }
    node1.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/994898p_moc.fits'
    node1.type = 'vos:DataNode'
    node2 = type('', (), {})()
    node2.props = {
        'date': '2020-09-13 19:55:03.067000+00:00',
        'size': 12,
        'MD5': 'ghi',
    }
    node2.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc/994899p_moc.fits'
    node2.type = 'vos:DataNode'

    node3 = type('', (), {})()
    node3.uri = 'vos://cadc.nrc.ca!vault/goliaths/moc'
    node3.type = 'vos:ContainerNode'
    node3.node_list = ['994898p_moc.fits', '994899p_moc.fits']

    return [node3, node1, node2]
