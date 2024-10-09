# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2024.                            (c) 2024.
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
#  Revision: 4
#
# ***********************************************************************
#

from datetime import datetime
from shutil import copyfile

from caom2pipe.caom_composable import Fits2caom2Visitor
from caom2pipe.manage_composable import StateRay, TaskType
from caom2pipe.ray_composable import ray_execution

from unittest.mock import patch

@patch('caom2pipe.ray_composable.exec_cmd')
@patch('caom2pipe.client_composable.ClientCollection')
def test_nominal_ray_execution(clients_mock, exec_cmd_mock, test_data_dir, test_config, tmp_path, change_test_dir):
    import logging
    logging.getLogger().setLevel(logging.DEBUG)
    test_config.change_working_directory(tmp_path)
    test_config.task_types = [TaskType.SCRAPE]
    test_config.data_sources = ['https://localhost:65432/rclone_listing']
    test_config.rclone_options = None
    test_config.interval = 40
    test_config.use_local_files = True
    test_config.logging_level = 'DEBUG'
    test_config.write_to_file(test_config)
    with open(test_config.proxy_file_name, 'w') as f:
        f.write('test content')

    state_ray = StateRay()
    test_start_time = datetime(2024, 10, 6, 1, 1, 1)
    test_end_time = datetime(2024, 10, 6, 2, 2, 2)
    state_ray.add_bookmark_start(test_config.data_sources[0], test_start_time)
    state_ray.add_bookmark_end(test_config.data_sources[0], test_end_time)
    state_ray.write_content(test_config.state_fqn)

    def _exec_mock(cmd):
        assert cmd in [
            f'rclone copy  --max-age=2024-10-06T01:41:01 --min-age={test_end_time.isoformat()} '
            f'--include=*.fits --http-url {test_config.data_sources[0]}/ :http: '
            f'{tmp_path}/2024-10-06T01_41_01_2024-10-06T02_02_02',
            f'rclone copy  --max-age={test_start_time.isoformat()} --min-age=2024-10-06T01:41:01 '
            f'--include=*.fits --http-url {test_config.data_sources[0]}/ :http: '
            f'{tmp_path}/2024-10-06T01_01_01_2024-10-06T01_41_01',
        ]
        if '--min-age=2024-10-06T01:41:01' in cmd:
            copyfile('/test_files/correct.fits', f'{tmp_path}/2024-10-06T01_01_01_2024-10-06T01_41_01/correct.fits')
    exec_cmd_mock.side_effect = _exec_mock

    test_data_visitors = []
    test_meta_visitors = [Fits2caom2Visitor]
    test_result = ray_execution(test_data_visitors, test_meta_visitors)
    assert test_result is not None, 'expect a result'
    assert test_result == 0, 'expect success'
    assert clients_mock.metadata_client.read.called, 'metadata read'
