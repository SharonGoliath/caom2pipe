# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2018.                            (c) 2018.
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

import math
import os
import pytest
import traceback

from datetime import datetime, timedelta, timezone
from dateutil import tz
from shutil import copy
from unittest.mock import Mock, patch

from caom2 import ProductType, ReleaseType, Artifact, ChecksumURI
from caom2 import SimpleObservation, ObservationIntentType, Algorithm
from caom2 import get_differences
from caom2pipe import manage_composable as mc

import test_conf as tc

TEST_STATE_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_state.yml')
TEST_OBS_FILE = os.path.join(tc.TEST_DATA_DIR, 'test_obs_id.fits.xml')
ISO8601_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


def test_read_write_obs_with_file():
    if os.path.exists(TEST_OBS_FILE):
        os.unlink(TEST_OBS_FILE)
    mc.write_obs_to_file(
        SimpleObservation(
            collection='test_collection',
            observation_id='test_obs_id',
            algorithm=Algorithm('exposure'),
        ),
        TEST_OBS_FILE,
    )
    test_subject = mc.read_obs_from_file(TEST_OBS_FILE)
    assert test_subject is not None, 'expect a result'
    assert isinstance(test_subject, SimpleObservation), 'wrong read'


def test_read_from_file():
    test_subject = mc.read_from_file(TEST_OBS_FILE)
    assert test_subject is not None, 'expect a result'
    assert isinstance(test_subject, list), 'wrong type of result'
    assert len(test_subject) == 8, 'missed some content'
    assert test_subject[0].startswith('<?xml version'), 'read failed'


def test_build_uri():
    test_subject = mc.build_uri('archive', 'file_name.fits')
    assert test_subject is not None, 'expect a result'
    assert test_subject == 'ad:archive/file_name.fits', 'wrong result'


def test_query_endpoint():
    with patch('requests.Session.get') as session_get_mock:
        test_result = mc.query_endpoint('https://localhost', timeout=25)
        assert test_result is not None, 'expected result'
        assert session_get_mock.called, 'mock not called'
        session_get_mock.assert_called_with('https://localhost', timeout=25)


def test_query_endpoint_session():
    session_mock = Mock()
    test_result = mc.query_endpoint_session(
        'https://localhost', session_mock, timeout=25
    )
    assert test_result is not None, 'expected result'
    assert session_mock.get.called, 'mock not called'
    session_mock.get.assert_called_with('https://localhost', timeout=25)


def test_config_class(tmp_path):
    config_content = f"""cache_file_name: cache.yml
cache_fqn: {tmp_path}/cache.yml
cleanup_files_when_storing: False
collection: NEOSSAT
data_source_extensions: ['.fits']
data_sources: []
failure_fqn: {tmp_path}/failure_log.txt
failure_log_file_name: failure_log.txt
features:
  run_in_airflow: True
  supports_catalog: True
  supports_composite: False
  supports_multiple_files: True
interval: 10
is_connected: True
log_file_directory: {tmp_path}
log_to_file: False
logging_level: DEBUG
observe_execution: False
progress_file_name: progress.txt
progress_fqn: {tmp_path}/progress.txt
proxy_file_name: test_proxy.pem
proxy_fqn: {tmp_path}/test_proxy.pem
recurse_data_sources: False
rejected_directory: {tmp_path}/test_config_dir
rejected_file_name: rejected.yml
rejected_fqn: {tmp_path}/test_config_dir/rejected.yml
report_fqn: {tmp_path}/data_report.txt
resource_id: ivo://cadc.nrc.ca/sc2repo
retry_count: 1
retry_failures: False
retry_file_name: retries.txt
retry_fqn: {tmp_path}/retries.txt
state_file_name: state.yml
state_fqn: {tmp_path}/state.yml
storage_inventory_resource_id: raven
store_modified_files_only: False
stream: raw
success_fqn: {tmp_path}/success_log.txt
success_log_file_name: success_log.txt
tap_id: ivo://cadc.nrc.ca/sc2tap
task_types:
  - visit
  - modify
use_local_files: False
work_file: todo.txt
work_fqn: {tmp_path}/todo.txt
working_directory: {tmp_path}
"""

    orig_cwd = os.getcwd()
    try:
        os.chdir(tmp_path)
        with open(f'{tmp_path}/config.yml', 'w') as f:
            f.write(config_content)

        test_config = mc.Config()
        test_config.get_executors()
        assert test_config is not None
        assert test_config.work_file == 'todo.txt'
        assert test_config.features is not None
        assert test_config.features.supports_composite is False, 'supports composite'
        assert test_config.working_directory == tmp_path.as_posix(), 'wrong dir'
        assert test_config.work_fqn == f'{tmp_path}/todo.txt', 'work_fqn'
        assert test_config.collection == 'NEOSSAT', 'collection'
        assert test_config.log_file_directory == tmp_path.as_posix(), 'logging dir'
        assert test_config.success_fqn == f'{tmp_path}/success_log.txt', 'success fqn'
        assert test_config.success_log_file_name == 'success_log.txt', 'success file'
        assert test_config.failure_fqn == f'{tmp_path}/failure_log.txt', 'failure fqn'
        assert test_config.failure_log_file_name == 'failure_log.txt', 'failure file'
        assert test_config.retry_file_name == 'retries.txt', 'retry file'
        assert test_config.retry_fqn == f'{tmp_path}/retries.txt', 'retry fqn'
        assert test_config.proxy_file_name == 'test_proxy.pem', 'proxy file name'
        assert test_config.proxy_fqn == f'{tmp_path}/test_proxy.pem', 'proxy fqn'
        assert test_config.state_file_name == 'state.yml', 'state file name'
        assert test_config.state_fqn == f'{tmp_path}/state.yml', 'state fqn'
        assert test_config.rejected_directory == f'{tmp_path}/test_config_dir', 'wrong rejected dir'
        assert test_config.rejected_file_name == 'rejected.yml', 'wrong rejected file'
        assert test_config.rejected_fqn == f'{tmp_path}/test_config_dir/rejected.yml', 'wrong rejected fqn'
        assert test_config.features.run_in_airflow is True, 'wrong runs in airflow'
        assert test_config.features.supports_catalog is True, 'wrong supports catalog'
        assert test_config.data_source_extensions == ['.fits'], 'extensions'
    finally:
        os.chdir(orig_cwd)


def test_exec_cmd():
    test_cmd = 'ls /abc'
    with pytest.raises(mc.CadcException):
        mc.exec_cmd(test_cmd)


def test_exec_cmd_redirect():
    fqn = os.path.join(tc.TEST_DATA_DIR, 'exec_cmd_redirect.txt')
    if os.path.exists(fqn):
        os.remove(fqn)

    test_cmd = 'ls'
    mc.exec_cmd_redirect(test_cmd, fqn)
    assert os.path.exists(fqn)
    assert os.stat(fqn).st_size > 0


def test_decompose_uri():
    test_uri = 'ad:STARS/galaxies.fits.gz'
    scheme, path, f_name = mc.decompose_uri(test_uri)
    assert scheme == 'ad', 'expected ad'
    assert path == 'STARS', 'expected STARS'
    assert f_name == 'galaxies.fits.gz', f'wrong f_name {f_name}'

    test_uri = 'vos:ngvs/merge/NGVS-2-4.m.z.Mg002.weight.fits.fz'
    scheme, path, f_name = mc.decompose_uri(test_uri)
    assert scheme == 'vos', 'expected vos'
    assert path == 'ngvs/merge', 'expected ngvs/merge'
    assert (
        f_name == 'NGVS-2-4.m.z.Mg002.weight.fits.fz'
    ), f'wrong f_name {f_name}'

    with pytest.raises(mc.CadcException):
        mc.decompose_uri('')


def test_read_csv_file():
    # bad read
    with pytest.raises(mc.CadcException):
        mc.read_csv_file(None)

    # good read
    test_file_name = os.path.join(tc.TEST_DATA_DIR, 'test_csv.csv')
    content = mc.read_csv_file(test_file_name)
    assert content is not None, 'empty results returned'
    assert len(content) == 1, 'missed the comment and the header'
    assert len(content[0]) == 24, 'missed the content'


def test_get_file_meta():
    # None
    with pytest.raises(mc.CadcException):
        mc.get_file_meta(None)

    # non-existent file
    fqn = os.path.join(tc.TEST_DATA_DIR, 'abc.txt')
    with pytest.raises(mc.CadcException):
        mc.get_file_meta(fqn)

    # empty file
    fqn = os.path.join(tc.TEST_DATA_DIR, 'todo.txt')
    if os.path.exists(fqn):
        os.unlink(fqn)
    open(fqn, 'w').close()
    result = mc.get_file_meta(fqn)
    assert result['size'] == 0, result['size']


def test_write_to_file():
    content = ['a.txt', 'b.jpg', 'c.fits.gz']
    test_fqn = f'{tc.TEST_DATA_DIR}/test_out.txt'
    if os.path.exists(test_fqn):
        os.remove(test_fqn)

    mc.write_to_file(test_fqn, '\n'.join(content))
    assert os.path.exists(test_fqn)


def test_get_artifact_metadata():
    test_fqn = os.path.join(tc.TEST_DATA_DIR, 'config.yml')
    test_uri = 'ad:TEST/config.yml'

    # wrong command line parameters
    with pytest.raises(mc.CadcException):
        mc.get_artifact_metadata(
            test_fqn, ProductType.WEIGHT, ReleaseType.META
        )

    # create action
    result = mc.get_artifact_metadata(
        test_fqn, ProductType.WEIGHT, ReleaseType.META, uri=test_uri
    )
    assert result is not None, 'expect a result'
    assert isinstance(result, Artifact), 'expect an artifact'
    assert result.product_type == ProductType.WEIGHT, 'wrong product type'
    assert result.content_length == 368, 'wrong length'
    assert result.content_checksum.uri == 'md5:e9db496ab9e875cc13ea52d4cc9db2c7', 'wrong checksum'

    # update action
    result.content_checksum = ChecksumURI('md5:abc')
    result = mc.get_artifact_metadata(
        test_fqn, ProductType.WEIGHT, ReleaseType.META, artifact=result
    )
    assert result is not None, 'expect a result'
    assert isinstance(result, Artifact), 'expect an artifact'
    assert result.content_checksum.uri == 'md5:e9db496ab9e875cc13ea52d4cc9db2c7', 'wrong checksum'


def test_state():
    if os.path.exists(TEST_STATE_FILE):
        os.unlink(TEST_STATE_FILE)
    with open(TEST_STATE_FILE, 'w') as f:
        f.write(
            'bookmarks:\n'
            '  gemini_timestamp:\n'
            '    last_record: 2019-07-23 20:52:03.524443\n'
            'context:\n'
            '  neossat_context:\n'
            '    - NEOSS\n'
            '    - 2020\n'
        )

    with pytest.raises(mc.CadcException):
        test_subject = mc.State('nonexistent', tz.UTC)

    test_subject = mc.State(TEST_STATE_FILE, tz.UTC)
    assert test_subject is not None, 'expect result'
    test_result = test_subject.get_bookmark('gemini_timestamp')
    assert test_result is not None, 'expect content'
    assert isinstance(test_result, datetime)

    test_context = test_subject.get_context('neossat_context')
    assert test_context is not None, 'expect a result'
    assert isinstance(test_context, list), 'wrong return type'
    assert len(test_context) == 2, 'wrong return length'
    assert 'NEOSS' in test_context, 'wrong content'
    test_context.append('2019')

    test_subject.save_state('gemini_timestamp', test_result + timedelta(3))
    test_subject.save_state('neossat_context', test_context)

    with open(TEST_STATE_FILE) as f:
        text = f.readlines()
        compare = ''.join(ii for ii in text)
        assert '2019-07-23' not in compare, 'content not updated'
        assert '2019' in compare, 'context content not updated'


def test_increment_time():
    t1 = '2017-06-26T17:07:21.527'
    t1_dt = datetime.strptime(t1, mc.ISO_8601_FORMAT)
    result = mc.increment_time(t1_dt, 10)
    assert result is not None, 'expect a result'
    assert result == datetime(2017, 6, 26, 17, 17, 21, 527000), 'wrong result'

    t2 = '2017-07-26T17:07:21.527'
    result = mc.increment_time(t2, 5)
    assert result is not None, 'expect a result'
    assert result == datetime(2017, 7, 26, 17, 12, 21, 527000), 'wrong result'

    t3 = 1571595618.0
    result = mc.increment_time(t3, 15)
    assert result == datetime(2019, 10, 20, 18, 35, 18), 'wrong t3 result'

    with pytest.raises(NotImplementedError):
        mc.increment_time(t1_dt, 23, '%f')


@patch('requests.get')
def test_http_get(mock_req):
    # Response mock
    class Object:
        def __init__(self):
            self.headers = {
                'Date': 'Thu, 25 Jul 2019 16:10:02 GMT',
                'Server': 'Apache',
                'Content-Length': '4',
                'Cache-Control': 'no-cache',
                'Expired': '-1',
                'Content-Disposition': 'attachment; filename="S20080610S0045.fits"',
                'Connection': 'close',
                'Content-Type': 'application/fits',
            }

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            return [b'aaa', b'bbb']

        def __enter__(self):
            return self

        def __exit__(self, a, b, c):
            return None

    test_object = Object()
    mock_req.return_value = test_object
    # wrong size
    with pytest.raises(mc.CadcException) as ex:
        mc.http_get('https://localhost/index.html', '/tmp/abc')

    assert mock_req.called, 'mock not called'
    assert 'File size error' in repr(ex.value), 'wrong failure'
    mock_req.reset_mock()

    # wrong checksum
    test_object.headers['Content-Length'] = 6
    test_object.headers['Content-Checksum'] = 'md5:abc'
    with pytest.raises(mc.CadcException) as ex:
        mc.http_get('https://localhost/index.html', '/tmp/abc')

    assert mock_req.called, 'mock not called'
    assert 'File checksum error' in repr(ex.value), 'wrong failure'
    mock_req.reset_mock()

    # checks succeed
    test_object.headers[
        'Content-Checksum'
    ] = '6547436690a26a399603a7096e876a2d'
    mc.http_get('https://localhost/index.html', '/tmp/abc')
    assert mock_req.called, 'mock not called'


def test_create_dir():
    test_f_name = f'{tc.TEST_DATA_DIR}/test_file_dir'
    if os.path.exists(test_f_name):
        if os.path.isdir(test_f_name):
            os.rmdir(test_f_name)
        else:
            os.unlink(test_f_name)

    with open(test_f_name, 'w') as f:
        f.write('test content')

    with pytest.raises(mc.CadcException):
        mc.create_dir(test_f_name)

    os.unlink(test_f_name)

    mc.create_dir(test_f_name)

    assert os.path.exists(test_f_name), 'should have been created'


@patch('cadcutils.net.ws.BaseWsClient.post')
@patch('cadcutils.net.ws.WsCapabilities.get_access_url')
def test_query_tap(caps_mock, base_mock, test_config):
    caps_mock.return_value = 'https://localhost'
    response = Mock()
    response.status_code = 200
    response.iter_content.return_value = [b'count\n' b'3212556\n']
    base_mock.return_value.__enter__.return_value = response
    test_config.tap_id = 'https://cadc.nrc.ca/sc2tap'
    result = mc.query_tap(
        'select count(*) from caom2.Observation',
        test_config.proxy_fqn,
        test_config.resource_id,
    )
    assert result is not None, 'expect a result'
    assert len(result) == 1, 'wrong amount of test data'
    assert result['count'] == 3212556, 'wrong test data'


def test_visit():

    class TestVisitor(mc.PreviewVisitor):

        def __init__(self, **kwargs):
            super().__init__(archive='VLASS', **kwargs)

        def generate_plots(self, obs_id):
            fqn = f'{self._working_dir}/{self._storage_name.prev}'
            with open(fqn, 'w') as f:
                f.write('test content')

            self.add_preview(
                self._storage_name.prev_uri,
                self._storage_name.prev,
                ProductType.THUMBNAIL,
                ReleaseType.META,
            )
            return 1

    class VisitStorageName(tc.TStorageName):
        mc.StorageName.collection = 'VLASS'

        def __init__(self):
            super().__init__(file_name=test_file_name)
            self._source_names = [self.file_uri]
            self.set_destination_uris()
            self._product_id = test_product_id

    test_config = mc.Config()
    test_config.collection = 'TEST'
    test_config.rejected_file_name = 'rejected.yml'
    test_config.rejected_directory = tc.TEST_DATA_DIR
    test_observable = mc.Observable(test_config)
    cadc_client_mock = Mock()
    clients_mock = Mock()
    clients_mock.data_client = cadc_client_mock
    test_product_id = 'VLASS1.2.T07t14.J084202-123000.quicklook.v1'
    test_file_name = (
        'VLASS1.2.ql.T07t14.J084202-123000.10.2048.v1.I.iter1.'
        'image.pbcor.tt0.subim.fits'
    )

    storage_name = VisitStorageName()
    kwargs = {
        'working_directory': tc.TEST_FILES_DIR,
        'clients': clients_mock,
        'stream': 'stream',
        'observable': test_observable,
        'storage_name': storage_name,
    }

    obs = mc.read_obs_from_file(f'{tc.TEST_DATA_DIR}/fpf_start_obs.xml')
    assert (
        len(obs.planes[test_product_id].artifacts) == 2
    ), 'initial condition'

    try:
        test_subject = TestVisitor(**kwargs)
        test_observation = test_subject.visit(obs)

        assert test_observation is not None, f'expect a result'

        check_number = 1
        end_artifact_count = 3
        expected_call_count = 1
        assert (
            test_subject.report['artifacts'] == check_number
        ), 'artifact not added'
        assert (
            len(obs.planes[test_product_id].artifacts) == end_artifact_count
        ), f'new artifacts'

        test_preview_uri = 'cadc:VLASS/test_obs_id_prev.jpg'
        assert (
            test_preview_uri in obs.planes[test_product_id].artifacts.keys()
        ), 'no preview'

        assert cadc_client_mock.put.called, 'put mock not called'
        assert (
            cadc_client_mock.put.call_count == expected_call_count
        ), 'put called wrong number of times'
        # it's an ad call, so there's a stream parameter
        cadc_client_mock.put.assert_called_with('/test_files', 'cadc:VLASS/test_obs_id_prev.jpg')
    except Exception as e:
        assert False, f'{str(e)}'
    # assert False


def test_config_write(tmpdir):
    """Test that TaskType read/write is working"""
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        test_config = mc.Config()
        test_config.working_directory = tmpdir
        test_config.logging_level = 'WARNING'
        test_config.collection = 'collection'
        scrape_found = False
        if mc.TaskType.SCRAPE in test_config.task_types:
            test_config.task_types = [mc.TaskType.VISIT, mc.TaskType.MODIFY]
            scrape_found = True
        else:
            test_config.task_types = [mc.TaskType.SCRAPE]
        test_config.write_to_file(test_config)
        second_config = mc.Config()
        second_config.get_executors()
        if scrape_found:
            assert mc.TaskType.VISIT in test_config.task_types, 'visit end'
            assert mc.TaskType.MODIFY in test_config.task_types, 'modify end'
        else:
            assert mc.TaskType.SCRAPE in test_config.task_types, 'scrape end'
    finally:
        os.chdir(orig_cwd)


def test_reverse_lookup():
    test_dict = {'a': 1, 'b': 2, 'c': 3}
    test_result = mc.reverse_lookup(3, test_dict)
    assert test_result is not None, 'expect a result'
    assert test_result == 'c', 'wrong result'
    test_result = mc.reverse_lookup(5, test_dict)
    assert test_result is None, 'value not in dict'


def test_cache(test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    test_config.cache_file_name = 'cache.yml'
    orig_cwd = os.getcwd()
    try:
        os.chdir(tmpdir)
        test_config.write_to_file(test_config)
        copy(f'{tc.TEST_DATA_DIR}/cache.yml', tmpdir)
        test_subject = mc.Cache()
        assert test_subject is not None, 'expect a return value'
        with pytest.raises(mc.CadcException):
            test_subject.get_from('not_found')
        test_subject = mc.Cache(rigorous_get=False)
        assert test_subject is not None, 'expect a return value'
        test_result = test_subject.get_from('not_found')
        assert test_result == [], 'expect no execution failure'
    finally:
        os.chdir(orig_cwd)


def test_value_repair_cache(test_config, tmpdir):
    test_config.change_working_directory(tmpdir)
    cache_file_name = 'value_repair_cache.yml'
    test_config.cache_file_name = cache_file_name
    test_config.write_to_file(test_config)
    test_cache_fqn = f'{tc.TEST_DATA_DIR}/{cache_file_name}'
    copy(test_cache_fqn, f'{tmpdir}/{cache_file_name}')

    test_subject = mc.ValueRepairCache()
    assert test_subject is not None, 'expect a result'

    test_observation = mc.read_obs_from_file(
        os.path.join(tc.TEST_DATA_DIR, 'value_repair_start.xml')
    )
    test_product_id = 'GN2001BQ013-04'
    test_artifact_uri = 'gemini:GEM/GN2001BQ013-04.fits'
    test_part = '0'
    test_chunk_index = 0
    test_plane = test_observation.planes[test_product_id]
    test_artifact = test_plane.artifacts[test_artifact_uri]
    test_part = test_artifact.parts[test_part]
    test_chunk = test_part.chunks[test_chunk_index]
    assert test_observation.type == 'Dark', 'repair initial condition'
    assert test_observation.proposal.pi_name == 'jjk', 'pi name ic'
    assert test_plane.meta_release == datetime(
        1990, 1, 1, 0, 0
    ), 'plane meta release ic'
    assert math.isclose(
        test_chunk.position.axis.function.ref_coord.coord1.pix,
        512.579594886106,
    ), 'position pix ic'
    assert test_artifact.uri == test_artifact_uri, 'artifact uri ic'
    assert test_part.product_type is ProductType.CALIBRATION, 'part ic'
    assert test_chunk.position.coordsys == 'ICRS', 'un-changed ic'
    assert test_observation.intent is None, 'None ic'
    assert test_observation.environment.seeing is None, 'None ic'
    assert test_chunk.position.axis.axis1.ctype == 'RA---TAN', 'unchanged ic'

    test_subject.repair(test_observation)

    assert test_observation.type == 'DARK', 'repair failed'
    assert (
        test_observation.proposal.pi_name == 'JJ Kavelaars'
    ), 'proposal pi name repair failed'
    assert test_plane.meta_release == datetime(
        2000, 1, 1, 0, 0
    ), 'plane meta release repair failed'
    assert math.isclose(
        test_chunk.position.axis.function.ref_coord.coord1.pix,
        512.57959987654321,
    ), 'position pix repair failed'
    assert (
        test_artifact.uri == 'cadc:GEMINI/GN2001BQ013-04.fits'
    ), f'uri repair failed {test_artifact.uri}'
    assert test_part.product_type is None, 'product type repair failed'

    # check that values that do not match are un-changed
    assert test_chunk.position.coordsys == 'ICRS', 'should be un-changed'
    # check that None values are not set - it's a _repair_ function, after
    # all, and the code cannot repair something that does not exist
    assert (
        test_observation.intent == ObservationIntentType.SCIENCE
    ), 'None value should be set, since "none" was the original type'
    assert (
        test_observation.environment.seeing is None
    ), 'None remains None because the original is a specific value'
    assert (
        test_chunk.position.axis.axis1.ctype == 'RA---TAN'
    ), 'unchanged post'

    with pytest.raises(mc.CadcException):
        # pre-condition of 'Unexpected repair key' error
        test_subject._value_repair = {'unknown': 'unknown'}
        test_subject.repair(test_observation)

    with pytest.raises(mc.CadcException):
        # try to set an attribute that cannot be assigned
        test_subject._value_repair = {
            'observation.instrument.name': {'gmos': 'GMOS-N'},
        }
        test_subject.repair(test_observation)

    # pre-condition of 'Could not figure out attribute name' the attribute is
    # not set in the test observation, so the observation should remain
    # unchanged
    test_observation = mc.read_obs_from_file(
        os.path.join(tc.TEST_DATA_DIR, 'value_repair_start.xml')
    )

    test_subject._value_repair = {'chunk.observable.dependent': 'not_found'}
    test_subject.repair(test_observation)

    test_compare_observation = mc.read_obs_from_file(
        os.path.join(tc.TEST_DATA_DIR, 'value_repair_start.xml')
    )
    test_diff = get_differences(test_compare_observation, test_observation)
    assert test_diff is None, 'expect no comparison error'


def test_extract_file_name_from_uri():
    # empty string
    test_result = mc.extract_file_name_from_uri('')
    assert test_result == '', 'wrong empty string'

    # uri
    test_result = mc.extract_file_name_from_uri('ad:TEST/abc.fits.gz')
    assert test_result == 'abc.fits.gz', 'wrong uri'

    # file name
    test_result = mc.extract_file_name_from_uri('abc.fits.gz')
    assert test_result == 'abc.fits.gz', 'wrong file name'

    # fqn
    test_result = mc.extract_file_name_from_uri(
        '/usr/src/app/data/abc.fits.gz'
    )
    assert test_result == 'abc.fits.gz', 'wrong fqn'


def test_use_vos():
    test_config = mc.Config()
    test_config.data_sources = []
    assert test_config.use_vos is False, 'empty data source'
    test_config.data_sources = ['vos:abc.fits']
    assert test_config.use_vos is True, 'one vos data source'
    test_config.data_sources = ['https://localhost', 'vos:abc.fits']
    assert test_config.use_vos is True, 'mixed data source'
    test_config.data_sources = ['/data/vos/2022', '/data/vos/2021']
    assert test_config.use_vos is False, 'use_local_files data source'


def test_log_directory_construction(test_config, tmpdir):
    test_config.log_to_file = True
    test_config.log_file_directory = tmpdir
    # reset the fqn's
    test_config.success_log_file_name = 'good.txt'
    test_config.failure_log_file_name = 'bad.txt'
    test_config.retry_file_name = 'again.txt'
    assert not os.path.exists(test_config.success_fqn)
    assert not os.path.exists(test_config.failure_fqn)
    assert not os.path.exists(test_config.retry_fqn)
    test_subject = mc.ExecutionReporter(test_config, observable=Mock(autospec=True))
    assert os.path.exists(test_config.success_fqn)
    assert os.path.exists(test_config.failure_fqn)
    assert os.path.exists(test_config.retry_fqn)

    try:
        raise RuntimeError('Read timed out')
    except Exception as e:
        test_sname = tc.TStorageName(obs_id='test_obs_id_2')
        test_subject.capture_failure(test_sname, e, traceback.format_exc())

    try:
        raise RuntimeError('some other error')
    except Exception as e2:
        test_sname = tc.TStorageName(obs_id='test_obs_id')
        test_subject.capture_failure(test_sname, e2, traceback.format_exc())

    test_subject.capture_success('test_obs_id', 'C121212_01234_CAL.fits.gz', datetime.now(tz=timezone.utc).timestamp())
    test_subject.report()

    success_content = open(test_config.success_fqn).read()
    assert 'test_obs_id C121212_01234_CAL.fits.gz' in success_content, 'wrong content'
    retry_content = open(test_config.retry_fqn).read()
    assert retry_content == '/tmp/test_file.fits.gz\n/tmp/test_file.fits.gz\n', f'{retry_content}'
    failure_content = open(test_config.failure_fqn).read()
    assert failure_content.endswith('some other error\n'), failure_content
    assert os.path.exists(test_config.rejected_fqn), test_config.rejected_fqn
    rejected_content = mc.read_as_yaml(test_config.rejected_fqn)
    assert rejected_content is not None, 'expect a result'
    test_result = rejected_content.get('bad_metadata')
    assert test_result is not None, 'wrong result'
    assert len(test_result) == 0, f'wrong number of entries {test_result}'


# - want to be timezone aware -
#   - options - use astropy.time
#             - use dateutil parser - this is more clever and can deal with more of the random formats that have been
#               encountered
def test_dt():
    # formats that have been encountered in FITS files
    for fmt in [
        '2024-03-23T10:08:31.123456',     # ISO_8601_FORMAT,
        '2024-03-23T10:08:31',            # '%Y-%m-%dT%H:%M:%S',
        '2024-03-23 10:08:31.123456',     # '%Y-%m-%d %H:%M:%S.%f',
        '23-Mar-2024 10:08',              # '%d-%b-%Y %H:%M',
        'Mar 23 2024',                    # '%b %d %Y',
        'Mar 23 10:08',                   # '%b %d %H:%M',
        '20240323-100831',                # '%Y%m%d-%H%M%S',
        '2024-03-23',                     # '%Y-%m-%d',
        'Fri Mar 23 10:08:31 HST 2024',   # '%a %b %d %H:%M:%S HST %Y',
        '2024/03/23 10:08:31',            # '%Y/%m/%d %H:%M:%S',
        'Fri, 23 Mar 2024 10:08:31 GMT',  # '%a, %d %b %Y %H:%M:%S GMT',
        '2024-03-23T10:08',               # '%Y-%m-%dT%H:%M',
        'Fri Mar 23 2024 10:08:31',       # '%a %b %d %Y %H:%M:%S',
        '20240323 10:08',                 # '%Y%m%d %H:%M',
        '23Mar2024 10:08',                # '%d%b%Y %H:%M',
        '2024-03-23 10:08',               # '%Y-%m-%d %H:%M',
        '23Mar24',                        # %y%b%d
        '2024/03/23',                     # '%Y/%m/%d',
        '23/03/24',                       # '%d/%m/%y',
        '23/03/24 10:08:31',              # '%d/%m/%y %H:%M:%S',
        '2024-03-23 10:08:31.123456+00:00',
        '10:08:31',                       # '%H:%M:%S',
        '2024/03/23,10:08:31',            # '%Y/%m/%d,%H:%M:%S',
        '2024_03_23T10_08_31.123456',     # '%Y_%m_%dT%H_%M_%S.%f',
        '2024-03-23HST10:08:31',          # '%Y-%m-%dHST%H:%M:%S',
        '2024Mar23',                      # %Y%b%d
    ]:
        result = mc.make_datetime(fmt)
        assert result is not None, f'expect a result {fmt}'
        if fmt != '10:08:31':  # get today's date with that hour
            assert (result.year == 2024 or result.year == 0), f'year {fmt}'
            assert (result.month == 3 or result.month == 0), f'month {fmt}'
            assert (result.day == 23 or result.day == 0), f'day {fmt} {result.day}'
        if 'HST' in fmt:
            assert (result.hour == 20 or result.hour == 0), f'hour {fmt}'
        else:
            assert (result.hour == 10 or result.hour == 0), f'hour {fmt}'
        assert (result.minute == 8 or result.minute == 0), f'minute {fmt}'
        assert (result.second == 31 or result.second == 0), f'second {fmt}'
        assert (result.microsecond == 123456 or result.microsecond == 0), f'micro {fmt}'
        assert result.tzname() is None, f'{fmt} {result.tzname()}'


def test_dt2():
    zone = tz.UTC
    for fmt in [
        datetime(2023, 3, 23, 10, 8, 31, 123456, tzinfo=zone),
        '2023-03-23 10:08:31.123456+00:00',
        1679566111.123456,
    ]:
        result = mc.make_datetime(fmt)
        assert result.year == 2023, f'year {fmt}'
        assert result.month == 3, f'month {fmt}'
        assert result.day == 23, f'day {fmt} {result.day}'
        assert result.hour == 10, f'hour {fmt}'
        assert result.minute == 8, f'minute {fmt}'
        assert result.second == 31, f'second {fmt}'
        assert result.microsecond == 123456, f'micro {fmt}'
        assert result.tzname() is None, f'tz {fmt} {result.tzname()}'
