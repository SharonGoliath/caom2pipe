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

import glob
import logging
import os
import shutil

from copy import deepcopy

from caom2pipe.data_source_composable import RemoteListDirDataSource
from caom2pipe.execute_composable import OrganizeExecutes
from caom2pipe.manage_composable import create_dir, make_datetime, TaskType
from caom2pipe.run_composable import TodoRunner
from caom2pipe.transfer_composable import CadcTransfer, Transfer


__all__ = ['ExecutionUnit', 'ExecutionUnitOrganizeExecutes']


class ExecutionUnit:
    """
    Could be:
    - 1 file
    - 1 rclone timebox
    - 1 group of files for horizontal scaling deployment

    Temporal Cohesion between logging setup/teardown and workspace setup/teardown.
    """

    def __init__(self, config, **kwargs):
        """
        :param root_directory str staging space location
        :param label str name of the execution unit. Should be unique and conform to posix directory naming standards.
        """
        self._log_fqn = None
        self._logging_level = None
        self._log_handler = None
        self._task_types = config.task_types
        self._config = config
        self._entry_dt = None
        self._clients = kwargs.get('clients')
        self._remote_metadata_reader = kwargs.get('metadata_reader')
        self._reporter = kwargs.get('reporter')
        self._observable = self._reporter.observable
        self._prev_exec_dt = kwargs.get('prev_exec_dt')
        self._exec_dt = kwargs.get('exec_dt')
        self._builder = kwargs.get('builder')
        self._meta_visitors = kwargs.get('meta_visitors')
        self._data_visitors = kwargs.get('data_visitors')
        self._local_metadata_reader = kwargs.get('staged_metadata_reader')
        self._label = (
            f'{self._prev_exec_dt.isoformat().replace(":", "_").replace(".", "_")}_'
            f'{self._exec_dt.isoformat().replace(":", "_").replace(".", "_")}'
        )
        self._working_directory = os.path.join(config.working_directory, self._label)
        if config.log_to_file:
            if config.log_file_directory:
                self._log_fqn = os.path.join(config.log_file_directory, self._label)
            else:
                self._log_fqn = os.path.join(config.working_directory, self._label)
            self._logging_level = config.logging_level
        self._num_entries = None
        self._central_wavelengths = {}  # key is original ObservationID, value is central wavelength
        self._observations = {}  # key is original ObservationID, values are Observation instances
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def entry_dt(self):
        return self._entry_dt

    @entry_dt.setter
    def entry_dt(self, value):
        self._entry_dt = make_datetime(value)

    @property
    def label(self):
        return self._label

    @property
    def num_entries(self):
        return self._num_entries

    @num_entries.setter
    def num_entries(self, value):
        self._num_entries = value

    @property
    def working_directory(self):
        return self._working_directory

    def do(self):
        """Make the execution unit one time-boxed copy from the DataSource to staging space, followed by a TodoRunner
        pointed to the staging space, and using that staging space with use_local_files: True. """
        self._logger.info(f'Begin do for {self._num_entries} entries in {self._label}')
        self._prepare()
        result = None
        # set a Config instance to use the staging space with 'use_local_files: True'
        todo_config = deepcopy(self._config)
        todo_config.use_local_files = True
        todo_config.data_sources = [self._working_directory]
        todo_config.recurse_data_sources = True
        self._logger.debug(f'do config for TodoRunner: {todo_config}')
        organizer = OrganizeExecutes(
            todo_config,
            self._meta_visitors,
            self._data_visitors,
            None,  # chooser
            Transfer(),
            CadcTransfer(self._clients.data_client),
            self._local_metadata_reader,
            self._clients,
            self._reporter,
        )
        local_data_source = RemoteListDirDataSource(todo_config)
        local_data_source.reporter = self._reporter
        # start a TodoRunner with the new Config instance, data_source, and metadata_reader
        todo_runner = TodoRunner(
            todo_config,
            organizer,
            builder=self._builder,
            data_sources=[local_data_source],
            metadata_reader=self._local_metadata_reader,
            reporter=self._reporter,
        )
        result = todo_runner.run()
        if todo_config.cleanup_files_when_storing:
            result |= todo_runner.run_retry()

        if local_data_source.num_entries != self._num_entries:
            self._logger.error(
                f'Expected to process {self._num_entries} entries, but found {local_data_source.num_entries} entries.'
            )
            result = -1
        self._logger.debug(f'End do with result {result}')
        return result

    def start(self):
        self._set_up_file_logging()
        self._create_workspace()

    def stop(self):
        self._clean_up_workspace()
        self._unset_file_logging()

    def _create_workspace(self):
        """Create the working area if it does not already exist."""
        self._logger.debug(f'Create working directory {self._working_directory}')
        create_dir(self._working_directory)

    def _clean_up_workspace(self):
        """Remove a directory and all its contents. Only do this if there is not a 'SCRAPE' task type, since the
        point of scraping is to be able to look at the pipeline execution artefacts once the processing is done.
        """
        if os.path.exists(self._working_directory) and TaskType.SCRAPE not in self._task_types:
            entries = glob.glob('*', root_dir=self._working_directory, recursive=True)
            if (self._config.cleanup_files_when_storing and len(entries) > 0) or len(entries) == 0:
                shutil.rmtree(self._working_directory)
                self._logger.error(f'Removed working directory {self._working_directory} and contents.')
        self._logger.debug('End _clean_up_workspace')

    def _prepare(self):
        self._logger.debug('Begin _prepare')
        work = glob.glob('**/*.fits', root_dir=self._working_directory, recursive=True)
        for file_name in work:
            self._logger.info(f'Working on {file_name}')
            for storage_name in self._remote_metadata_reader._storage_names.values():
                if storage_name.file_name == os.path.basename(file_name):
                    original_fqn = os.path.join(self._working_directory, file_name)
                    self._remote_metadata_reader.set_headers(storage_name, original_fqn)
                    break

        self._logger.debug('End _prepare')

    def _set_up_file_logging(self):
        """Configure logging to a separate file for each execution unit.

        If log_to_file is set to False, don't create a separate log file for each entry, because the application
        should leave as small a logging trace as possible.
        """
        if self._log_fqn and self._logging_level:
            self._log_handler = logging.FileHandler(self._log_fqn)
            formatter = logging.Formatter('%(asctime)s:%(levelname)s:%(name)-12s:%(lineno)d:%(message)s')
            self._log_handler.setLevel(self._logging_level)
            self._log_handler.setFormatter(formatter)
            logging.getLogger().addHandler(self._log_handler)

    def _unset_file_logging(self):
        """Turn off the logging to the separate file for each entry being
        processed."""
        if self._log_handler:
            logging.getLogger().removeHandler(self._log_handler)
            self._log_handler.flush()
            self._log_handler.close()


class ExecutionUnitOrganizeExecutes(OrganizeExecutes):
    """A class to do nothing except be "not None" when called."""

    def __init__(self):
        pass

    def choose(self):
        # do nothing for the over-arching StateRunner
        pass

    def do_one(self, _):
        raise NotImplementedError
