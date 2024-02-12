# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2021.                            (c) 2021.
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
This module contains classes and functions that have to do with managing
and using long-term connections to CADC services.

Leave out:
- get_artifact_metadata_client, since it mostly CAOM2 dependent, and the
  only client-based part is get_cadc_meta_client, which is already here
- should Metrics be here? Maybe.
"""

import logging
import os
import traceback

from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO, StringIO
from sys import getsizeof

from astropy.table import Table

from cadctap import CadcTapClient
from cadcutils import net, exceptions
from cadcdata import FileInfo
from caom2utils.data_util import StorageClientWrapper
from caom2utils.data_util import get_file_encoding, get_file_type
from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc
from caom2repo import CAOM2RepoClient

__all__ = [
    'client_get',
    'client_put_fqn',
    'ClientCollection',
    'current',
    'data_get',
    'declare_client',
    'define_subject',
    'get_cadc_headers_client',
    'get_cadc_meta_client',
    'get_cadc_meta_client_v',
    'query_tap_client',
    'repo_create',
    'repo_delete',
    'repo_get',
    'repo_update',
    'si_client_get',
    'si_client_get_headers',
    'si_client_info',
    'si_client_put',
    'vault_info',
]


class ClientCollection:
    """
    This class initializes and provides accessors to known HTTP clients
    for CADC services.

    This is time-dependent cohesiveness. The clients should all be declared
    at approximately the same time, will live as long as the pipeline
    instance, and require the same AAI credential information, even though
    there is one per not-related-at-all CADC service.

    Eventually session retries, etc, might be configurable, but that need has
    not yet been demonstrated.
    """

    def __init__(self, config):
        self._metadata_client = None
        self._data_client = None
        self._query_client = None
        self._vo_client = None
        self._metrics = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self._init(config)

    @property
    def data_client(self):
        return self._data_client

    @property
    def metadata_client(self):
        return self._metadata_client

    @property
    def metrics(self):
        return self._metrics

    @metrics.setter
    def metrics(self, value):
        if self._data_client is not None:
            self._data_client._metrics = value
        self._metrics = value

    @property
    def query_client(self):
        return self._query_client

    @property
    def vo_client(self):
        return self._vo_client

    @vo_client.setter
    def vo_client(self, value):
        self._vo_client = value

    def _init(self, config):
        if mc.TaskType.SCRAPE in config.task_types:
            self._logger.info(
                f'SCRAPE\'ing data - no clients will be initialized.'
            )
        else:
            subject = define_subject(config)
            self._metadata_client = CAOM2RepoClient(
                subject, config.logging_level, config.resource_id
            )
            self._data_client = declare_client(config)
            if config.tap_id is not None:
                self._query_client = CadcTapClient(
                    subject=subject, resource_id=config.tap_id
                )


def client_get(client, working_directory, file_name, source, metrics):
    """
    Retrieve a local copy of a file available from CADC. Assumes the working
    directory location exists and is writeable.

    :param client: The Client for read access to CADC storage.
    :param working_directory: Where 'file_name' will be written.
    :param file_name: What to copy from CADC storage.
    :param source: Where to retrieve the file from.
    :param metrics: track success execution times, and failure counts.
    """
    start = current()
    fqn = os.path.join(working_directory, file_name)
    try:
        retrieved_size = client.copy(source, destination=fqn)
    except Exception as e:
        metrics.observe_failure('copy', 'vos', file_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Did not retrieve {fqn} because {e}')
    end = current()
    metrics.observe(start, end, retrieved_size, 'copy', 'vos', file_name)


def client_put_fqn(client, source_name, destination_name, metrics=None):
    """
    Make a copy of a locally available file by writing it to CADC. Assumes
    file and directory locations are correct.

    :param client: Client for write access to CADC storage.
    :param source_name: fully-qualified file name on local storage
    :param destination_name: Where to write the file.
    :param metrics: Tracking success execution times, and failure counts.
    """
    start = current()
    try:
        stored_size = client.copy(source_name, destination=destination_name)
    except Exception as e:
        metrics.observe_failure('copy', 'vos', os.path.basename(source_name))
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Failed to store data with {e}')
    end = current()
    metrics.observe(
        start, end, stored_size, 'copy', 'vos', os.path.basename(source_name)
    )


def current():
    """Encapsulate returning UTC now in microsecond resolution."""
    return datetime.now(tz=timezone.utc).timestamp()


def data_get(client, working_directory, file_name, archive, metrics):
    """
    Retrieve a local copy of a file available from CADC. Assumes the working
    directory location exists and is writeable.

    :param client: The CadcDataClient for read access to CADC storage.
    :param working_directory: Where 'file_name' will be written.
    :param file_name: What to copy from CADC storage.
    :param archive: Which archive to retrieve the file from.
    :param metrics: track success execution times, and failure counts.
    """
    start = current()
    fqn = os.path.join(working_directory, file_name)
    try:
        client.get_file(archive, file_name, destination=fqn)
        if not os.path.exists(fqn):
            raise mc.CadcException(
                f'ad retrieve failed. {fqn} does not exist.'
            )
    except Exception as e:
        metrics.observe_failure('get', 'data', file_name)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Did not retrieve {fqn} because {e}')
    end = current()
    file_size = os.stat(fqn).st_size
    metrics.observe(start, end, file_size, 'get', 'data', file_name)


def declare_client(config, metrics=None):
    """Common code to set the client used for interacting with CADC
    storage."""
    subject = define_subject(config)
    cadc_client = StorageClientWrapper(
        resource_id=config.storage_inventory_resource_id, subject=subject, metrics=metrics
    )
    return cadc_client


def define_subject(config):
    """Common code to figure out which credentials to use based on the
    content of a Config instance."""
    subject = None
    if config.proxy_fqn is not None and os.path.exists(config.proxy_fqn):
        logging.debug(
            f'Using proxy certificate {config.proxy_fqn} for credentials.'
        )
        subject = net.Subject(username=None, certificate=config.proxy_fqn)
    else:
        logging.warning(f'Proxy certificate is {config.proxy_fqn}.')
        raise mc.CadcException(
            'No credentials provided (proxy certificate). '
            'Cannot create an anonymous subject.'
        )
    return subject


def get_cadc_headers_client(archive, file_name, client):
    """
    Creates the FITS headers object by fetching the FITS headers of a CADC
    file. The function takes advantage of the fhead feature of the CADC
    storage service and retrieves just the headers and no data, minimizing
    the transfer time.

    The file may be public or proprietary, depending on the capabilities of
    the supplied client parameter.

    :param archive: CADC Archive reference, as a string
    :param file_name: CADC Archive file name, as a string. Includes compression
        and file type extensions.
    :param client: CadcDataClient instance.
    :return: a string of keyword/value pairs.
    """
    b = BytesIO()
    b.name = file_name
    client.get_file(archive, file_name, b, fhead=True)
    fits_header = b.getvalue().decode('ascii')
    b.close()
    return fits_header


def get_cadc_meta_client(client, archive, fname):
    """
    Retrieve metadata for a single file from the original CADC storage
    system.

    :param client: CadcDataClient instance
    :param archive: archive file has been stored to
    :param fname: name of file in the archive
    :return: FileMeta
    """
    temp = client.get_file_info(archive, fname)
    return FileMeta(temp.get('size'), temp.get('md5sum'))


@dataclass
class FileMeta:
    """The bits of information about a file that are used to decide whether
    or not to transfer, and whether or not a transfer was successful."""

    f_size: int
    md5sum: str


def get_cadc_meta_client_v(storage_name, cadc_client):
    """
    Retrieve metadata for a single file. This implementation uses a
    vos.Client.

    This is a very bad implementation, but there is no information on what
    might be a better one.

    :param storage_name: Artifact URI
    :param cadc_client:
    :return: FileMeta
    """
    node = cadc_client.get_node(storage_name, limit=None, force=False)
    f_size = node.props.get('length')
    f_md5sum = node.props.get('MD5')
    return FileMeta(f_size, f_md5sum)


def query_tap_client(query_string, tap_client, timeout=10):
    """
    :param query_string ADQL
    :param tap_client which client to query the service with
    :param timeout int value of minutes to wait for a query to complete
    :returns an astropy votable instance."""

    logging.debug(f'query_tap_client: execute query \n{query_string}')
    buffer = StringIO()
    tap_client.query(
        query_string,
        output_file=buffer,
        data_only=True,
        response_format='tsv',
        timeout=timeout,
    )
    return Table.read(buffer.getvalue().split('\n'), format='ascii.tab')


def repo_create(client, observation, metrics):
    start = current()
    try:
        client.create(observation)
    except Exception as e:
        metrics.observe_failure('create', 'caom2', observation.observation_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not create an observation record for '
            f'{observation.observation_id}. {e}'
        )
    end = current()
    if metrics is not None:
        metrics.observe(
            start,
            end,
            getsizeof(observation),
            'create',
            'caom2',
            observation.observation_id,
        )


def repo_delete(client, collection, obs_id, metrics):
    start = current()
    try:
        client.delete(collection, obs_id)
    except Exception as e:
        metrics.observe_failure('delete', 'caom2', obs_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not delete the observation record for {obs_id}. {e}'
        )
    end = current()
    if metrics is not None:
        metrics.observe(start, end, 0, 'delete', 'caom2', obs_id)


def repo_get(client, collection, obs_id, metrics):
    start = current()
    try:
        observation = client.read(collection, obs_id)
    except exceptions.NotFoundException:
        observation = None
    except Exception as e:
        if metrics is not None:
            metrics.observe_failure('read', 'caom2', obs_id)
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not retrieve an observation record for {obs_id} '
            f'because {e}.'
        )
    end = current()
    if metrics is not None:
        metrics.observe(
            start, end, getsizeof(observation), 'read', 'caom2', obs_id
        )
    return observation


def repo_update(client, observation, metrics):
    start = current()
    try:
        client.update(observation)
    except Exception as e:
        if metrics is not None:
            metrics.observe_failure(
                'update', 'caom2', observation.observation_id
            )
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Could not update an observation record for '
            f'{observation.observation_id}. {e}'
        )
    end = current()
    if metrics is not None:
        metrics.observe(
            start,
            end,
            getsizeof(observation),
            'update',
            'caom2',
            observation.observation_id,
        )


def si_client_info(client, source):
    """
    Retrieve metadata for a file available from CADC using the
    StorageInventory client.

    :param client: The Client for read access to CADC storage.
    :param source: Artifact URI - where to retrieve the file from.
    """
    try:
        result = client.info(source)
    except exceptions.NotFoundException as e1_ignore:
        logging.info(f'cadcinfo:: {source} not found')
        result = None
    except Exception as e2:
        logging.error(f'cadcinfo failed for {e2}.')
        logging.debug(traceback.format_exc())
        raise e2
    return result


def si_client_get(client, fqn, source, metrics):
    """
    Retrieve a local copy of a file available from CADC using the
    StorageInventory client. Assumes the working directory location exists
    and is writeable. Checks that the md5sum of the retrieved file is the
    same as the md5sum of the file at CADC.

    :param client: The Client for read access to CADC storage.
    :param fqn: str fully-qualified name to which the retrieved file will be
        written.
    :param source: Artifact URI - where to retrieve the file from.
    :param metrics: track success execution times, and failure counts.
    """
    start = current()
    try:
        client.cadcget(source, dest=fqn)
        if not os.path.exists(fqn):
            raise mc.CadcException(f'Retrieve failed. {fqn} does not exist.')
        local_meta = mc.get_file_meta(fqn)
        cadc_meta = si_client_info(client, source)
        if local_meta.get('md5sum') != cadc_meta.md5sum:
            raise mc.CadcException(
                f'Wrong MD5 checksum {local_meta.get("md5sum")} retrieved for '
                f'{source}.'
            )
    except Exception as e:
        if metrics is not None:
            metrics.observe_failure('cadcget', 'si', os.path.basename(fqn))
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Did not retrieve {fqn} because {e}')
    if metrics is not None:
        end = current()
        metrics.observe(
            start,
            end,
            local_meta.get('size'),
            'cadcget',
            'si',
            os.path.basename(fqn),
        )


def si_client_get_headers(client, storage_name):
    """
    Creates the FITS headers object by fetching the FITS headers of a file
    from CADC. The function takes advantage of the fhead feature of the CADC
    storage service and retrieves just the headers and no data, minimizing
    the transfer time.

    :param client: The Client for read access to CADC storage.
    :param storage_name: Artifact URI - where to retrieve the file from.
    :return: a string of keyword/value pairs.
    """
    try:
        b = BytesIO()
        b.name = storage_name
        client.cadcget(storage_name, b, fhead=True)
        fits_header = b.getvalue().decode('ascii')
        b.close()
        return ac.make_headers_from_string(fits_header)
    except Exception as e:
        logging.debug(traceback.format_exc())
        raise mc.CadcException(
            f'Did not retrieve {storage_name} header ' f'because {e}'
        )


def si_client_put(client, fqn, storage_name, metrics):
    """
    Make a copy of a locally available file by writing it to CADC. Assumes
    file and directory locations are correct.

    Uses StorageInventoryClient to check the md5sum of the file stored is
    the same as the md5sum of the file on disk.

    :param client: Client for write access to CADC storage.
    :param fqn: str fully-qualified name from which the file will be
        stored.
    :param storage_name: Artifact URI - the label for storing the file.
    :param metrics: Tracking success execution times, and failure counts.
    """
    start = current()
    replace = True
    cwd = os.getcwd()
    try:
        cadc_meta = si_client_info(client, storage_name)
        os.chdir(os.path.dirname(fqn))
        local_meta = mc.get_file_meta(fqn)
        if cadc_meta is None:
            replace = False
        client.cadcput(
            storage_name,
            src=fqn,
            replace=replace,
            file_type=local_meta.get('type'),
            file_encoding=get_file_encoding(storage_name),
            md5_checksum=local_meta.get('md5sum'),
        )
    except Exception as e:
        metrics.observe_failure('cadcput', 'si', os.path.basename(fqn))
        logging.debug(traceback.format_exc())
        raise mc.CadcException(f'Failed to store data with {e}')
    finally:
        os.chdir(cwd)
    end = current()
    metrics.observe(
        start,
        end,
        local_meta.get('size'),
        'cadcput',
        'si',
        os.path.basename(fqn),
    )


def vault_info(client, uri):
    """
    Translate Node properties into FileInfo.

    :param client: Vault client
    :param uri: VOS URI
    :return: an instance of FileInfo
    """
    try:
        file_type = get_file_type(uri)
        encoding = get_file_encoding(uri)
        node = client.get_node(uri, limit=None, force=False)
        return FileInfo(
            id=uri,
            size=mc.to_int(node.props.get('length')),
            md5sum=node.props.get('MD5').replace('md5:', ''),
            lastmod=node.props.get('lastmod'),
            file_type=file_type,
            encoding=encoding,
        )
    except exceptions.NotFoundException as e:
        return None
