# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2022.                            (c) 2022.
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
#  : 4 $
#
# ***********************************************************************
#

"""
Integration testing that pytest makes easy, between:
 the run_composable (TodoRunner, StateRunner),
 data_source_composable (particularly LocalFilesDataSource and VaultCleanupDataSource), and
 manage_composable refactoring of ExecutionReporter and ExecutionSummary

from the previous locations across the DataSource specializations, the OrganizeExecutor, and the *Runner
specializations.
"""

import logging


from datetime import datetime
from os import chdir, getcwd

from cadcdata import FileInfo
from caom2pipe import data_source_composable as dsc
from caom2pipe.manage_composable import CadcException, Config, State, TaskType
from caom2pipe.transfer_composable import VoScienceTransfer
from caom2pipe import run_composable as rc
from traceback import format_exc

from unittest.mock import call, Mock, patch, PropertyMock
from test_data_source_composable import _create_dir_listing

TEST_START_TIME = datetime(year=2020, month=3, day=3, hour=1, minute=1, second=1)
TEST_END_TIME = datetime(year=2020, month=3, day=3, hour=2, minute=1, second=1)


def test_report_output_todo_local(test_config, tmpdir):
    # LocalFilesDataSource - incremental and all
    # work is all the list of files,
    #   one has been stored already,
    #   one fails fitsverify
    #   one fails ingestion
    #   one succeeds

    orig_cwd = getcwd()
    try:
        chdir(tmpdir)
        test_config.logging_level = 'ERROR'
        test_config.task_types = [TaskType.STORE]
        test_config.interval = 100
        test_config.use_local_files = True
        test_config.data_sources = ['/tmp']
        test_config.cleanup_failure_destination = '/data/failure'
        test_config.cleanup_success_destination = '/data/success'
        test_config.change_working_directory(tmpdir)

        pre_success_listing, file_info_list = _create_dir_listing('/tmp', 4)
        test_reader = Mock()

        test_clients = Mock()

        for clean_up in [True, False]:
            for store_modified in [True, False]:
                move_calls = [
                    call('/tmp/A1.fits', '/data/failure'),
                    call('/tmp/A0.fits', '/data/success'),
                    call('/tmp/A2.fits', '/data/failure'),
                    call('/tmp/A3.fits', '/data/success'),
                ]
                put_side_effect = [None, CadcException, None]
                info_side_effect = [file_info_list[0], None, file_info_list[0], file_info_list[0], file_info_list[0]]
                file_info_side_effect = [file_info_list[0], file_info_list[2], file_info_list[3]]
                skipped_sum = 0
                if store_modified:
                    move_calls = [
                        call('/tmp/A0.fits', '/data/success'),
                        call('/tmp/A1.fits', '/data/failure'),
                        call('/tmp/A2.fits', '/data/failure'),
                        call('/tmp/A3.fits', '/data/success'),
                    ]
                    put_side_effect = [CadcException, None]
                    info_side_effect = [file_info_list[0], None, None, None, file_info_list[0]]
                    file_info_side_effect = [file_info_list[0], file_info_list[3], None, None]
                    skipped_sum = 1
                if not clean_up:
                    put_side_effect = [None, CadcException, None]
                    skipped_sum = 0
                test_config.cleanup_files_when_storing = clean_up
                test_config.store_modified_files_only = store_modified
                test_data_source = dsc.LocalFilesDataSource(
                    test_config, test_clients.data_client, test_reader, recursive=True, scheme='cadc'
                )
                type(test_data_source).end_dt = PropertyMock(return_value=TEST_END_TIME)
                Config.write_to_file(test_config)
                State.write_bookmark(test_config.state_fqn, test_config.bookmark, TEST_START_TIME)
                for state in [True, False]:
                    (
                        test_config,
                        test_clients,
                        test_builder,
                        test_data_sources,
                        test_reader,
                        test_organizer,
                        test_reporter,
                    ) = rc.common_runner_init(
                        config=test_config,
                        clients=test_clients,
                        name_builder=None,
                        sources=[test_data_source],
                        modify_transfer=None,
                        metadata_reader=test_reader,
                        state=state,
                        store_transfer=None,
                        meta_visitors=[],
                        data_visitors=[],
                        chooser=None,
                    )
                    # 0 stored already
                    # 1 fails fitsverify
                    # 2 fails ingestion
                    # 3 succeeds
                    test_clients.data_client.info.side_effect = info_side_effect
                    test_clients.data_client.put.side_effect = put_side_effect
                    test_reader.file_info.get.side_effect = file_info_side_effect
                    verify_mock = Mock()
                    # FITS files are valid, invalid, valid, valid
                    verify_mock.side_effect = [True, False, True, True]
                    test_data_source._verify_file = verify_mock
                    if state:
                        test_subject = rc.StateRunner(
                            test_config,
                            test_organizer,
                            test_builder,
                            test_data_sources,
                            test_reader,
                            test_reporter,
                        )
                    else:
                        test_subject = rc.TodoRunner(
                            test_config,
                            test_organizer,
                            test_builder,
                            test_data_sources,
                            test_reader,
                            test_reporter,
                        )

                    diagnostic = f'clean up {clean_up} store modified {store_modified} subject {type(test_subject)}'
                    with patch('os.scandir') as scandir_mock:
                        scandir_mock.return_value.__enter__.return_value = pre_success_listing
                        move_mock = Mock()
                        test_subject._data_sources[0]._move_action = move_mock
                        try:
                            test_result = test_subject.run()
                        except Exception as e:
                            logging.error(format_exc())
                            assert False, f'{e} {diagnostic}'

                        assert test_result is not None, f'expect a result {diagnostic}'
                        assert test_result == -1, f'there are some failures {diagnostic}'
                        if clean_up:
                            assert move_mock.called, f'move should be called {diagnostic}'
                            assert move_mock.call_count == 4, f'wrong move call count {diagnostic}'
                            move_mock.assert_has_calls(move_calls)
                        else:
                            assert not move_mock.called, f'move should not be called {diagnostic}'
                        test_report = test_subject._data_sources[0]._reporter._summary
                        assert test_report._entries_sum == 4, f'entries {diagnostic} {test_report.report()}'
                        assert test_report._success_sum == 2, f'success {diagnostic} {test_report.report()}'
                        assert test_report._timeouts_sum == 0, f'timeouts {diagnostic} {test_report.report()}'
                        assert test_report._retry_sum == 0, f'retry {diagnostic} {test_report.report()}'
                        assert test_report._errors_sum == 2, f'errors {diagnostic} {test_report.report()}'
                        assert test_report._rejected_sum == 1, f'rejection {diagnostic} {test_report.report()}'
                        assert test_report._skipped_sum == skipped_sum, f'skipped {diagnostic} {test_report.report()}'
    finally:
        chdir(orig_cwd)


@patch('caom2pipe.astro_composable.check_fitsverify')
def test_report_output_todo_vault(verify_mock, test_config, tmpdir):
    # VaultCleanupDataSource - incremental and all
    # work is all the list of files,
    #   one has been stored already,
    #   one fails fitsverify
    #   one fails ingestion
    #   one succeeds

    orig_cwd = getcwd()
    try:
        chdir(tmpdir)
        test_config.task_types = [TaskType.STORE]
        test_config.interval = 100
        test_config.change_working_directory(tmpdir)
        test_config.logging_level = 'ERROR'
        test_config.use_local_files = False
        test_config.data_sources = ['vos://cadc.nrc.ca!vault/goliaths/test']
        test_config.cleanup_failure_destination = 'vos://cadc.nrc.ca!vault/goliaths/test/failure'
        test_config.cleanup_success_destination = 'vos://cadc.nrc.ca!vault/goliaths/test/success'

        ignore_pre_success_listing, file_info_list = _create_dir_listing('/tmp', 4)
        vo_listing, vo_file_info_list, vo_listdir = _get_node_listing(4)
        test_reader = Mock()
        test_reader.file_info = {
            'cadc:OMM/A0.fits': file_info_list[0],
            'cadc:OMM/A1.fits': file_info_list[1],
            'cadc:OMM/A2.fits': file_info_list[2],
            'cadc:OMM/A3.fits': file_info_list[3],
        }
        test_clients = Mock()

        for clean_up in [True, False]:
            for store_modified in [True, False]:
                put_side_effect = [None, CadcException, None]
                info_side_effect = [file_info_list[0], None, None, file_info_list[0]]
                skipped_sum = 0
                if store_modified:
                    put_side_effect = [CadcException, None]
                    info_side_effect = [file_info_list[0], None, None, None, None, None, file_info_list[3]]
                    skipped_sum = 1
                test_config.cleanup_files_when_storing = clean_up
                test_config.store_modified_files_only = store_modified
                if clean_up:
                    store_transfer = VoScienceTransfer(test_clients.vo_client)
                else:
                    store_transfer = VoScienceTransfer(test_clients.vo_client)
                    put_side_effect = [None, CadcException, None]

                test_data_source = dsc.VaultCleanupDataSource(
                    test_config, test_clients.vo_client, test_clients.data_client, test_reader
                )
                type(test_data_source).end_dt = PropertyMock(return_value=TEST_END_TIME)
                Config.write_to_file(test_config)
                State.write_bookmark(test_config.state_fqn, test_config.bookmark, TEST_START_TIME)
                # state == True is incremental operation
                for state in [True, False]:
                    (
                        test_config,
                        test_clients,
                        test_builder,
                        test_data_sources,
                        test_reader,
                        test_organizer,
                        test_reporter,
                    ) = rc.common_runner_init(
                        config=test_config,
                        clients=test_clients,
                        name_builder=None,
                        sources=[test_data_source],
                        modify_transfer=None,
                        metadata_reader=test_reader,
                        state=state,
                        store_transfer=store_transfer,
                        meta_visitors=[],
                        data_visitors=[],
                        chooser=None,
                    )
                    # 0 stored already
                    # 1 fails fitsverify
                    # 2 fails ingestion
                    # 3 succeeds
                    test_clients.data_client.info.side_effect = info_side_effect
                    test_clients.data_client.put.side_effect = put_side_effect
                    test_clients.vo_client.isdir.return_value = False
                    test_clients.vo_client.listdir.side_effect = vo_listdir
                    if not state:
                        test_clients.vo_client.listdir.side_effect = [vo_listdir.node_list]
                    test_clients.vo_client.get_node.side_effect = vo_listing
                    if store_modified:
                        # FITS files are valid, invalid, valid, valid, but the verify is called in the transfer, so
                        # the first 'valid' check never happens
                        verify_mock.side_effect = [False, True, True]
                    else:
                        verify_mock.side_effect = [True, False, True, True]

                    if state:
                        test_subject = rc.StateRunner(
                            test_config,
                            test_organizer,
                            test_builder,
                            test_data_sources,
                            test_reader,
                            test_reporter,
                        )
                    else:
                        test_subject = rc.TodoRunner(
                            test_config,
                            test_organizer,
                            test_builder,
                            test_data_sources,
                            test_reader,
                            test_reporter,
                        )

                    diagnostic = f'clean up {clean_up} store modified {store_modified} subject {type(test_subject)}'
                    ds_move_mock = Mock()
                    test_subject._data_sources[0]._move_action = ds_move_mock
                    transfer_move_mock = Mock()
                    store_transfer._move_action = transfer_move_mock
                    try:
                        test_result = test_subject.run()
                    except Exception as e:
                        logging.error(format_exc())
                        assert False, f'{e} {diagnostic}'

                    assert test_result is not None, f'expect a result {diagnostic}'
                    assert test_result == -1, f'there are some failures {diagnostic}'
                    # the move mock is always called - there's an 'if cleanup_when_storing check in the method itself
                    if clean_up:
                        assert ds_move_mock.called, f'ds move should be called {diagnostic}'
                        assert ds_move_mock.call_count == 4, f'ds wrong move call count {diagnostic}'
                        ds_move_mock.assert_has_calls([
                            call(
                                'vos://cadc.nrc.ca!vault/goliaths/test/A0.fits',
                                'vos://cadc.nrc.ca!vault/goliaths/test/success',
                            ),
                            call(
                                'vos://cadc.nrc.ca!vault/goliaths/test/A1.fits',
                                'vos://cadc.nrc.ca!vault/goliaths/test/failure',
                            ),
                            call(
                                'vos://cadc.nrc.ca!vault/goliaths/test/A2.fits',
                                'vos://cadc.nrc.ca!vault/goliaths/test/failure',
                            ),
                            call(
                                'vos://cadc.nrc.ca!vault/goliaths/test/A3.fits',
                                'vos://cadc.nrc.ca!vault/goliaths/test/success',
                            ),
                        ])
                    else:
                        if store_modified:
                            assert ds_move_mock.called, f'ds move should be called {diagnostic}'
                            assert ds_move_mock.call_count == 1, f'ds wrong move call count {diagnostic}'
                            ds_move_mock.assert_called_with(
                                'vos://cadc.nrc.ca!vault/goliaths/test/A0.fits',
                                'vos://cadc.nrc.ca!vault/goliaths/test/success',
                            )
                        else:
                            assert not ds_move_mock.called, 'ds move mock not called'
                    test_report = test_subject._data_sources[0]._reporter._summary
                    assert test_report._entries_sum == 4, f'entries {diagnostic} {test_report.report()}'
                    assert test_report._success_sum == 2, f'success {diagnostic} {test_report.report()}'
                    assert test_report._timeouts_sum == 0, f'timeouts {diagnostic} {test_report.report()}'
                    assert test_report._retry_sum == 0, f'retry {diagnostic} {test_report.report()}'
                    assert test_report._errors_sum == 2, f'errors {diagnostic} {test_report.report()}'
                    assert test_report._rejected_sum == 0, f'rejection {diagnostic} {test_report.report()}'
                    assert test_report._skipped_sum == skipped_sum, f'skipped {diagnostic} {test_report.report()}'
    finally:
        chdir(orig_cwd)


def _get_node_listing(count, prefix='A'):
    listing_result = []
    file_info_list = []
    listdir_result = []

    listing_node = type('Node', (), {})()
    listing_node.uri = 'vos://cadc.nrc.ca!vault/goliaths/test'
    listing_node.type = 'vos:ContainerNode'
    listing_result.append(listing_node)

    for ii in range(0, count):
        node = type('Node', (), {})()
        dt = datetime.fromtimestamp(1583197266.0 + 5.0 * ii).isoformat()
        node.props = {
            'size': 123,
            'MD5': 'md5:abc',
            'date': dt,
        }
        node.uri = f'{listing_node.uri}/{prefix}{ii}.fits'
        node.type = 'vos:DataNode'
        listdir_result.append(f'{prefix}{ii}.fits')
        listing_result.append(node)
        file_info_list.append(FileInfo(id=node.uri, size=123, md5sum='md5:abc'))

    listing_node.node_list = listdir_result
    return listing_result, file_info_list, listing_node
