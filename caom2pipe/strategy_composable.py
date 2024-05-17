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
#  $Revision: 4 $
#
# ***********************************************************************
#

import logging
import re
import traceback

from os.path import basename
from urllib.parse import urlparse

from caom2pipe.manage_composable import build_uri, search_for_file


class HierarchyStrategy:
    """
    This class encapsulates:
    - naming rules for a collection, for example:
        - file name case in storage,
        - naming pattern enforcement
    - cardinality rules for a collection. Specialize for creation support of:
        - observation_id
        - product_id
        - artifact URI
        - preview URI
        - thumbnail URI
    - source_names: fully-qualified name of a file at it's source, if required.
      This may be a Linux directory+file name, and HTTP URL, or an IVOA Virtual
      Storage URI.
    - the information required to make the cardinality decisions
    """

    # string value for Observation.collection
    collection = None
    # regular expression that can be used to determine if a file name or
    # observation id meets particular patterns.
    collection_pattern = '.*'
    # string value for the scheme of the file URI. Defaults to the fall-back
    # scheme for Storage Inventory
    scheme = 'cadc'
    preview_scheme = 'cadc'
    data_source_extensions = ['.fits', '.fits.gz', '.fits.header']

    def __init__(self, entry, source_names, metadata=None, file_info=None):
        """
        """
        self._obs_id = None
        self._product_id = None
        self._source_names = source_names
        # list of str - the Artifact URIs as represented at CADC. Sufficient for storing/retrieving to/from CADC.
        self._destination_uris = []
        self._file_name = basename(entry)
        # str - the file name with all file type and compression extensions removed
        self._file_id = self._file_name
        for extension in HierarchyStrategy.data_source_extensions:
            self._file_id = self._file_id.replace(extension, '')
        self._file_info = file_info
        # might be a FITS header, might be a TAP query, might be an HDF5 attribute walk result
        self._metadata = metadata
        self._logger = logging.getLogger(self.__class__.__name__)
        self.set_destination_uris()
        self.set_obs_id()
        self.set_product_id()
        self._logger.debug(self)

    def __str__(self):
        return (
            f'\n'
            f'          obs_id: {self.obs_id}\n'
            f'       file_name: {self.file_name}\n'
            f'        file_uri: {self.file_uri}\n'
            f'      product_id: {self.product_id}\n'
            f'    source_names: {self.source_names}\n'
            f'destination_uris: {self.destination_uris}'
        )

    def _get_uri(self):
        return build_uri(scheme=HierarchyStrategy.scheme, archive=HierarchyStrategy.collection, file_name=self._file_name)

    @property
    def destination_uris(self):
        return self._destination_uris

    @property
    def file_id(self):
        """The file name with all file type and compression extensions removed"""
        return self._file_id

    @property
    def file_info(self):
        return self._file_info

    @file_info.setter
    def file_info(self, value):
        self._file_info = value

    @property
    def file_name(self):
        return self._file_name

    @property
    def file_uri(self):
        """The CADC Storage URI for the file."""
        return build_uri(
            scheme=HierarchyStrategy.scheme,
            archive=HierarchyStrategy.collection,
            file_name=self._file_name.replace('.gz', '').replace('.bz2', '').replace('.header', ''),
        )

    @property
    def hdf5(self):
        return HierarchyStrategy.is_hdf5(self._file_name)

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, value):
        self._metadata = value

    @property
    def obs_id(self):
        """The observation ID associated with the file name."""
        return self._obs_id

    @property
    def prev(self):
        """The preview file name for the file."""
        return f'{self._obs_id}_prev.jpg'

    @property
    def prev_uri(self):
        """The preview URI."""
        return build_uri(scheme=HierarchyStrategy.preview_scheme, archive=HierarchyStrategy.collection, file_name=self.prev)

    @property
    def product_id(self):
        """The relationship between the observation ID of an observation, and
        the product ID of a plane."""
        return self._product_id

    @property
    def source_names(self):
        return self._source_names

    @source_names.setter
    def source_names(self, value):
        self._source_names = value

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return f'{self._obs_id}_prev_256.jpg'

    @property
    def thumb_uri(self):
        """The thumbnail URI."""
        return build_uri(scheme=HierarchyStrategy.preview_scheme, archive=HierarchyStrategy.collection, file_name=self.thumb)

    def is_valid(self):
        """:return True if the observation ID conforms to naming rules."""
        pattern = re.compile(HierarchyStrategy.collection_pattern)
        return pattern.match(self._file_name)

    def get_file_fqn(self, working_directory):
        return search_for_file(self, working_directory)

    def set_destination_uris(self):
        for entry in self._source_names:
            temp = urlparse(entry)
            if '.fits' in entry:
                self._destination_uris.append(
                    build_uri(
                        scheme=HierarchyStrategy.scheme,
                        archive=HierarchyStrategy.collection,
                        file_name=basename(temp.path).replace('.gz', '').replace('.bz2', '').replace('.header', ''),
                    )
                )
            else:
                self._destination_uris.append(
                    build_uri(HierarchyStrategy.scheme, HierarchyStrategy.collection, basename(temp.path))
                )

    def set_obs_id(self, **kwargs):
        if self._obs_id is None:
            self._obs_id = self._file_id

    def set_product_id(self, **kwargs):
        if self._product_id is None:
            self._product_id = self._file_id

    @staticmethod
    def is_hdf5(entry):
        return '.hdf5' in entry or '.h5' in entry

    @staticmethod
    def is_preview(entry):
        return '.jpg' in entry


class HierarchyStrategyContext:
    """This class takes one execution unit, and does the work, usually external to CADC, to make the HierarchyStrategy
    instances for it."""

    def __init__(self, clients):
        self._hierarchies = {}
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def hierarchies(self):
        return self._hierarchies

    def expand(self, entry):
        """Takes one entry and turns that into n hierarchies. """
        raise NotImplementedError

    def unset(self, keys):
        for key in keys:
            self._hierarchies.pop(key)
            self._logger.debug(f'Removing {key} from list of storage names.')
