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

import logging
import tempfile
import traceback

from cadcutils import exceptions
from caom2utils import data_util
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc


__all__ = [
    'DelayedClientReader',
    'FileMetadataReader',
    'Hdf5FileMetadataReader',
    'MetadataReader',
    'reader_factory',
    'StorageClientReader',
    'VaultReader',
]


class MetadataReader:
    """Wrap the mechanism for retrieving metadata from the data source, that is used to create a
    CAOM2 record, and to make decisions about how to create that record. Use
    cases are:
        - FITS files on local disk
        - CADC storage client
        - Gemini http client
        - VOSpace client

    Users of this class hierarchy should be able to reduce the number of
    times file headers and FileInfo are retrieved for the same file.

    Use the source for determining FileInfo information because comparing
    the md5sum at the source to CADC storage is how to determine whether or
    not a file needs to be pushed to CADC for storage, should storing files be
    part of the execution.

    TODO - how to handle thumbnails and previews
    """

    def __init__(self):
        # dicts are indexed by mc.StorageName.destination_uris
        self._headers = {}  # astropy.io.fits.Headers
        self._file_info = {}  # cadcdata.FileInfo
        self._logger = logging.getLogger(self.__class__.__name__)

    def __str__(self):
        file_info_keys = '\n'.join(ii for ii in self._file_info.keys())
        header_keys = '\n'.join(ii for ii in self._headers.keys())
        return f'\nheaders:\n{header_keys}\nfile_info:\n{file_info_keys}'

    @property
    def file_info(self):
        return self._file_info

    @property
    def headers(self):
        return self._headers

    def _retrieve_file_info(self, key, source_name):
        """
        :param key: Artifact URI
        :param source_name: fully-qualified name at the data source
        """
        raise NotImplementedError

    def _retrieve_headers(self, key, source_name):
        """
        :param key: Artifact URI
        :param source_name: fully-qualified name at the data source
        """
        raise NotImplementedError

    def set(self, storage_name):
        """Retrieves the Header and FileInfo information to memory."""
        self._logger.debug(f'Begin set for {storage_name.file_name}')
        self.set_headers(storage_name)
        self.set_file_info(storage_name)
        self._logger.debug('End set')

    def set_file_info(self, storage_name):
        """Retrieves FileInfo information to memory."""
        self._logger.debug(f'Begin set_file_info for {storage_name.file_name}')
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._file_info:
                self._logger.debug(f'Retrieve FileInfo for {entry}')
                self._retrieve_file_info(entry, storage_name.source_names[index])
        self._logger.debug('End set_file_info')

    def set_headers(self, storage_name):
        """Retrieves the Header information to memory."""
        self._logger.debug(f'Begin set_headers for {storage_name.file_name}')
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._headers:
                self._logger.debug(f'Retrieve headers for {entry}')
                self._retrieve_headers(entry, storage_name.source_names[index])
        self._logger.debug('End set_headers')

    def reset(self):
        self._headers = {}
        self._file_info = {}
        self._logger.debug('End reset')

    def unset(self, storage_name):
        """Remove an entry from the collections. Keeps memory usage down over long runs."""
        for entry in storage_name.destination_uris:
            if entry in self._headers:
                del self._headers[entry]
            if entry in self._file_info:
                del self._file_info[entry]
            self._logger.debug(f'Unset the metadata for {entry}')


class FileMetadataReader(MetadataReader):
    """Use case: FITS files on local disk."""

    def __init__(self):
        super().__init__()

    def _retrieve_file_info(self, key, source_name):
        self._file_info[key] = data_util.get_local_file_info(source_name)

    def _retrieve_headers(self, key, source_name):
        self._headers[key] = []
        if '.fits' in source_name:
            try:
                self._headers[key] = data_util.get_local_headers_from_fits(source_name)
            except OSError as e:
                self._headers[key] = data_util.get_local_file_headers(source_name)


class Hdf5FileMetadataReader(FileMetadataReader):

    def __init__(self):
        super().__init__()
        self._descriptors = {}
        self._working_directory = None

    @property
    def descriptors(self):
        return self._descriptors

    @property
    def working_directory(self):
        return self._working_directory

    @working_directory.setter
    def working_directory(self, value):
        self._working_directory = value

    def set_file_info(self, storage_name):
        """Retrieves FileInfo information to memory."""
        self._logger.debug(f'Begin set_file_info for {storage_name.file_name}')
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._file_info:
                self._logger.debug(
                    f'Retrieve FileInfo for {entry} {storage_name.get_file_fqn(self._working_directory)}'
                )
                self._retrieve_file_info(entry, storage_name.get_file_fqn(self._working_directory))
        self._logger.debug('End set_file_info')

    def set_headers(self, storage_name):
        """Retrieves the Header information to memory."""
        self._logger.debug(f'Begin set_headers for {storage_name.file_name}')
        for index, entry in enumerate(storage_name.destination_uris):
            if entry not in self._headers:
                if storage_name.hdf5:
                    self._logger.debug(f'Retrieve hdf5 headers for {entry}')
                    self._retrieve_headers(entry, storage_name.get_file_fqn(self._working_directory))
                elif '.fits' in entry:
                    self._logger.debug(f'Retrieve fits headers for {entry}')
                    super()._retrieve_headers(entry, storage_name.get_file_fqn(self._working_directory))
                else:
                    self._headers[entry] = []
        self._logger.debug('End set_headers')

    def _retrieve_headers(self, key, fqn):
        self._logger.debug(f'Retrieve "attrs" for {fqn}')
        if key not in self._descriptors:
            # local import to limit exposure in Docker builds
            import h5py
            f_in = h5py.File(fqn)
            self._descriptors[key] = f_in
            # Laurie Rosseau-Nepton - 26-04-23
            # The standard_spectrum is related to flux calibration used on the data. The other one is for the science
            # data and is the one that should be used.
            if len(f_in.attrs) > 50:
                self._logger.debug(f'Found attrs for {fqn}')
                self._headers[key] = [f_in.attrs]
            else:
                self._logger.warning(f'No attrs for {fqn}.')
                self._headers[key] = []

    def reset(self):
        super().reset()
        for descriptor in self._descriptors.values():
            descriptor.close()
        self._descriptors = {}

    def unset(self, storage_name):
        super().unset(storage_name)
        for entry in storage_name.destination_uris:
            if entry in self._descriptors:
                self._descriptors[entry].close()
                del self._descriptors[entry]
                self._logger.debug(f'Unsetting descriptors for {entry}')


class StorageClientReader(MetadataReader):
    """Use case: CADC storage.

    The storage_name.destination_uris are always the CADC storage reference,
    so if this specialization is being used, rely on that naming,
    instead of using the source names, which is the default implementation.
    """

    def __init__(self, client):
        """
        :param client: StorageClientWrapper instance
        """
        super().__init__()
        self._client = client

    def _retrieve_file_info(self, key, source_name):
        self._file_info[key] = self._client.info(source_name)

    def _retrieve_headers(self, key, source_name):
        self._headers[key] = []
        if '.fits' in source_name:
            self._headers[key] = self._client.get_head(source_name)

    def set_file_info(self, storage_name):
        """Retrieves FileInfo information from CADC storage to memory."""
        self._logger.debug(f'Begin set_file_info for {storage_name.file_name}')
        for entry in storage_name.destination_uris:
            if entry not in self._file_info:
                self._retrieve_file_info(entry, entry)
        self._logger.debug('End set_file_info')

    def set_headers(self, storage_name):
        """Retrieves the Header information from CADC storage to memory."""
        self._logger.debug(f'Begin set_headers for {storage_name.file_name}')
        for entry in storage_name.destination_uris:
            if entry not in self._headers:
                self._retrieve_headers(entry, entry)
        self._logger.debug('End set_headers')


class DelayedClientReader(StorageClientReader):
    """Use case: TaskType.STORE, with use_local_files set to False.

    The objective is to be able to delay the retrieval of the FileInfo and header information until the file is
    retrieved from the data provider, and stored at CADC. If the files are retrieved from a remote location, for
    example by http or ftp, the STORE task needs to be executable without the MetadataReader causing an execution
    failure.
    """

    def _retrieve_file_info(self, key, source_name):
        """Retrieves FileInfo information to memory. Ignore retrieval failures, as the file may not yet be at CADC.
        """
        temp = self._client.info(source_name)
        if temp is None:
            self._logger.debug(f'Ignore failure to find FileInfo for {source_name}')
        else:
            self._file_info[key] = temp

    def _retrieve_headers(self, key, source_name):
        """Retrieves the Header information to memory. Ignore retrieval failures, as the file may not yet be at CADC.
        """
        if '.fits' in source_name:
            try:
                self._headers[key] = self._client.get_head(source_name)
            except exceptions.UnexpectedException as e:
                # the record was not found, this is expected, keep going
                self._logger.debug(f'Ignore UnexpectedException for {source_name} {e}')
                pass
        else:
            self._headers[key] = []


class VaultReader(MetadataReader):
    """Use case: vault."""

    def __init__(self, client):
        """
        :param client: vos.Client instance
        """
        super().__init__()
        self._client = client

    def _retrieve_file_info(self, key, source_name):
        self._file_info[key] = clc.vault_info(self._client, source_name)

    def _retrieve_headers(self, key, source_name):
        if '.fits' in source_name:
            try:
                tmp_file = tempfile.NamedTemporaryFile()
                self._client.copy(source_name, tmp_file.name, head=True)
                temp_header = data_util.get_local_file_headers(tmp_file.name)
                tmp_file.close()
                self._headers[key] = temp_header
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                raise mc.CadcException(f'Did not retrieve {source_name} header because {e}')
        else:
            self._headers[key] = []


def reader_factory(config, clients):
    if config.use_local_files or mc.TaskType.SCRAPE in config.task_types:
        metadata_reader = FileMetadataReader()
    elif config.use_vos and clients.vo_client is not None:
        metadata_reader = VaultReader(clients.vo_client)
    elif mc.TaskType.STORE in config.task_types and not config.use_local_files:
        metadata_reader = DelayedClientReader(clients.data_client)
    else:
        metadata_reader = StorageClientReader(clients.data_client)
    logging.debug(f'Returning {metadata_reader.__class__.__name__} metadata_reader.')
    return metadata_reader
