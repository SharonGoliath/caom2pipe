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

import logging

from os.path import basename
from caom2pipe import manage_composable as mc

__all__ = [
    'builder_factory',
    'EntryBuilder',
    'GuessingBuilder',
    'ObsIDBuilder',
    'StorageNameInstanceBuilder',
    'StorageNameBuilder',
]


class StorageNameBuilder:
    """
    The mechanisms for translating a list of work into a collection-specific
    name (otherwise known as which class instantiates the extension of the
    manage_composable.StorageName class) are:
    - this class directly
    - a builder class provided by the collection implementation. This allows
        for a delay in StorageName construction to when processing on an
        entry is about to begin, for the case where StorageName construction
        is more expensive execution-time or storage-wise
    - the collection class directly. This choice is usually masked behind
        a specialization of the data_source_composable.get_work class.
    """

    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def build(self, entry):
        """
        The default implementation assumes the entry is already in the form
        of a StorageName instance, and does nothing.

        :param entry: StorageName instance
        :return: entry the same StorageName instance
        """
        return entry


class StorageNameInstanceBuilder(StorageNameBuilder):
    def __init__(self, config, scheme='cadc'):
        super().__init__()
        mc.StorageName.scheme = scheme
        mc.StorageName.collection = config.collection

    def build(self, entry):
        return mc.StorageName(
            obs_id=basename(mc.StorageName.remove_extensions(entry)),
            file_name=basename(entry),
            source_names=[entry],
        )


class EntryBuilder(StorageNameBuilder):
    """
    A class that assumes constructing the StorageName instance requires a
    single positional parameter.
    """

    def __init__(self, storage_name):
        super().__init__()
        self._storage_name = storage_name

    def build(self, entry):
        return self._storage_name(entry)


class ObsIDBuilder(StorageNameBuilder):
    """
    A class that assumes constructing the StorageName instance requires a
    single parameter: a str 'obs_id'
    """

    def __init__(self, storage_name):
        super().__init__()
        self._storage_name = storage_name

    def build(self, entry):
        return self._storage_name(obs_id=entry)


class GuessingBuilder(StorageNameBuilder):
    """
    A class that attempts to guess the StorageName instance parameters,
    based on what's provided as input.
    """

    def __init__(self, storage_name):
        super().__init__()
        self._storage_name = storage_name

    def build(self, entry):
        self._logger.debug(f'Build a {self._storage_name.__name__} instance with {entry}.')
        return self._storage_name(file_name=(basename(entry)), source_names=[entry])


def builder_factory(config):
    result = StorageNameInstanceBuilder(config)
    logging.debug(f'Create {result.__class__.__name__} builder')
    return result
