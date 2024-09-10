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
import logging
import os

from dateutil import tz

from cadcutils.net import Subject
from cadctap import CadcTapClient
from caom2pipe.manage_composable import Config, query_tap_pandas
from caom2pipe.run_composable import set_logging

__all__ = ['Validator', 'VALIDATE_OUTPUT']

VALIDATE_OUTPUT = 'validated.yml'


class Validator:
    """
    Compares the state of CADC storage entries with the state of the data source. Identifies files that are at CADC
    that do not exist at the source, and files at the source that are not represented in CADC storage.

    Also checks that the timestamp associated with the file at the source is less than the CADC storage timestamp.

    CADC is the destination and the files originate from the data source.

    The method 'read_from_source' must be implemented for validate to
    run to completion.

    ##########     a,b        ##########
    #        #     ->         #        #
    # SOURCE #                #  DATA  #
    #        #     <-         #        #
    ##########     c          ##########

    a => the files at the data source that are not at CADC
    b => the files at the data source that are newer than files at CADC
    c => the files at CADC that are not at the data source

    Metadata cross-validation with data at CADC is the provenance of various "artifact-diff" flavours.
    """

    def __init__(self, source_name, scheme='cadc', preview_suffix='jpg'):
        """
        :param source_name: String value used for logging
        :param scheme: string which encapsulates scheme as used in Artifact URIs.
        :param preview_suffix String value that is excluded from queries, usually for files that are produced at CADC.
        """
        self._config = Config()
        self._config.get_executors()
        set_logging(self._config)
        subject = Subject(certificate=self._config.proxy_fqn)
        self._data_client = CadcTapClient(subject=subject, resource_id=self._config.storage_inventory_tap_resource_id)
        self._source_missing = []
        self._destination_missing = []
        self._destination_older = []
        self._source_name = source_name
        self._scheme = scheme
        self._preview_suffix = preview_suffix
        self._logger = logging.getLogger(self.__class__.__name__)
        if self._config.log_to_file and os.path.exists(self._config.log_file_directory):
            log_fqn = os.path.join(self._config.log_file_directory, f'{self._source_name}_validate_log.txt')
            log_handler = logging.FileHandler(log_fqn)
            formatter = logging.Formatter(
                '%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s'
            )
            log_handler.setLevel(self._config.logging_level)
            log_handler.setFormatter(formatter)
            logging.getLogger().addHandler(log_handler)

    def _find_unaligned_dates(self, source, data):
        import pandas as pd
        newer = pd.DataFrame()
        if len(data) > 0 and len(source) > 0:
            # SG - 08-09-22 - All times in the SI databases are in UTC.
            #
            # source_temp => url, dt, f_name
            # data => uri, contentLastModified
            data['f_name'] = data.uri.apply(Validator.filter_column)
            data['dt_d'] = data.contentLastModified.apply(Validator.make_utc_aware)
            source['dt_2'] = source.dt.apply(Validator.make_utc_aware)
            merged = pd.merge(source, data, how='inner', on=['f_name'])
            newer = merged[merged.dt_d < merged.dt_2]
        return newer

    def _read_list_from_destination_data(self):
        """Code to execute a query for files and the arrival time, that are in
        CADC storage.
        """
        query = (
            f"SELECT A.uri, A.contentLastModified "
            f"FROM inventory.Artifact AS A "
            f"WHERE A.uri LIKE '%:{self._config.collection}/%' "
            f"AND A.uri NOT LIKE '%{self._preview_suffix}'"
        )
        self._logger.debug(f'Query is {query}')
        temp = query_tap_pandas(query, self._data_client)
        temp['f_name'] = temp.uri.apply(Validator.filter_column)
        # columns are uri, contentLastModified, f_name
        return temp

    def read_from_source(self):
        """Read the entire source site listing. This function is expected to
        return a pandas.DataFrame with at least columns url, dt (datetime64[ns]), and f_name."""
        raise NotImplementedError()

    def validate(self):
        # SOURCE
        self._logger.info('Query source data.')
        source_temp = self.read_from_source()
        source_temp_fqn = os.path.join(self._config.working_directory, 'source_temp.txt')
        source_temp.to_csv(source_temp_fqn, header=False, index=False)

        # DATA
        self._logger.info('Query destination data.')
        dest_data_temp = self._read_list_from_destination_data()

        # SOURCE vs DATA
        self._logger.info('Find files that do not appear at CADC.')
        # a => files at SOURCE that aren't in DATA
        self._destination_missing = Validator.find_missing(dest_data_temp, source_temp, 'f_name')

        self._logger.info(f'Find files that do not appear at {self._source_name}.')
        # b => files in DATA that aren't at SOURCE
        self._source_missing = Validator.find_missing(source_temp, dest_data_temp, 'f_name')

        self._logger.info(f'Find files that are newer at {self._source_name} than at CADC.')
        # c => files newer at SOURCE than in DATA
        self._destination_older = self._find_unaligned_dates(source_temp, dest_data_temp)

        self._logger.info('Log the results.')
        source_missing_fqn = os.path.join(self._config.working_directory, 'not_at_cadc.txt')
        dest_missing_fqn = os.path.join(self._config.working_directory, f'not_at_{self._source_name}.txt')
        newer_timestamps_fqn = os.path.join(self._config.working_directory, f'newer_at_{self._source_name}.txt')
        self._source_missing.to_csv(source_missing_fqn, header=False, index=False)
        self._destination_missing.to_csv(dest_missing_fqn, header=False, index=False)
        self._destination_older.to_csv(newer_timestamps_fqn, header=False, index=False)

        self._logger.info(
            f'Results:\n'
            f'  - {len(self._source_missing)} files at {self._source_name} that are '
            f'not referenced by CADC storage entries\n'
            f'  - {len(self._destination_missing)} storage entries at CADC that do '
            f'not reference {self._source_name} files\n'
            f'  - {len(self._destination_older)} files that are newer at '
            f'{self._source_name} than in CADC storage'
        )
        return self._source_missing, self._destination_missing, self._destination_older

    def write_todo(self):
        """Write a todo.txt file, given the list of entries available from
        the source, that are not currently at the destination (CADC)."""
        raise NotImplementedError()

    @staticmethod
    def filter_column(value):
        # meta is usually a URI, and want just a file name
        return value.split('/')[-1].replace('.gz', '')

    @staticmethod
    def find_missing(in_here, from_there, column):
        return in_here[~in_here[column].isin(from_there[column])]

    @staticmethod
    def make_utc_aware(a):
        import pandas as pd
        return pd.to_datetime(a, utc=True)
