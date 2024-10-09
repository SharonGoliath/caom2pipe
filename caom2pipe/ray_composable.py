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

import logging
import ray
import traceback

from datetime import datetime
from os import scandir
from os.path import basename, join

from caom2utils.data_util import get_local_file_headers, get_local_file_info
from caom2pipe.astro_composable import check_fitsverify
from caom2pipe.data_source_composable import ListDirSeparateDataSource
from caom2pipe.execute_composable import OrganizeExecutesRay
from caom2pipe.manage_composable import Config, create_dir, exec_cmd, ExecutionReporterRay, increment_time, StateRay
from caom2pipe.manage_composable import StorageName
from caom2pipe.run_composable import TodoRunner


# @ray.remote
def do_one_ray(entry, organizer):
    result = None
    try:
        result = organizer.do_one(entry)
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        result = -1
    return result


class Y(StorageName):

    def __init__(self, source_names):
        super().__init__(file_name=basename(source_names[0]), source_names=source_names)

    def set_file_id(self, **kwargs):
        self._file_id = basename(StorageName.remove_extensions(self._source_names[0]))


class HttpStagingDataSource(ListDirSeparateDataSource):
    """rclone from an http source"""

    def __init__(self, config, start_dt, end_dt, data_source_key, reporter):
        super().__init__(config)
        self._start_dt = start_dt
        self._end_dt = end_dt
        if data_source_key[-1] == '/':
            self._data_source = data_source_key
        else:
            self._data_source = f'{data_source_key}/'
        self._label = (
            f'{start_dt.isoformat().replace(":", "_").replace(".", "_")}_'
            f'{end_dt.isoformat().replace(":", "_").replace(".", "_")}'
        )
        self._working_directory = join(config.working_directory, self._label)
        create_dir(self._working_directory)
        self._include_pattern = ','.join(f'*{ii}' for ii in config.data_source_extensions)
        self._reporter = reporter

    def _append_work(self, entry):
        with scandir(entry) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    self._append_work(entry.path)
                else:
                    if self.default_filter(entry):
                        y = Y([entry.path])
                        y._file_info = get_local_file_info(entry.path)
                        y._metadata = get_local_file_headers(entry.path)
                        self._logger.debug(f'Adding {y} to work list.')
                        self._work.append(y)

    def _is_valid(self, path):
        return True

    def default_filter(self, entry):
        work_with_file = False
        if check_fitsverify(entry.path) and self._is_valid(entry.path):
            work_with_file = True
        return work_with_file

    def _stage(self):
        self._logger.debug(f'Begin _stage from {self._start_dt} to {self._end_dt}')
        rclone_options_str = self._config.rclone_options if self._config.rclone_options else ''
        # get the files from the DataSource to the staging space
        exec_cmd(
            f'rclone copy {rclone_options_str} --max-age={self._start_dt.isoformat()} '
            f'--min-age={self._end_dt.isoformat()} --include={self._include_pattern} --http-url '
            f'{self._data_source} :http: {self._working_directory}'
        )
        self._logger.debug('End _stage')

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        self._stage()
        self._append_work(self._working_directory)
        self._capture_todo()
        self._logger.debug('End get_work')
        return self._work


class RayTodoRunner(TodoRunner):

    def __init__(self, config, organizer, reporter, start_dt, end_dt, data_source_key):
        # TODO - need to clean up the staging directories created here?
        self._stager = HttpStagingDataSource(config, start_dt, end_dt, data_source_key, reporter)
        super().__init__(
            config=config,
            organizer=organizer,
            builder=None,
            data_sources=[self._stager],
            metadata_reader=None,
            reporter=reporter,
        )
        self._entries = []

    @property
    def num_entries(self):
        return self._num_entries

    def _build_todo_list(self, data_source):
        self._entries = self._stager.get_work()
        self._num_entries = len(self._entries)

    def _process_entry(self):
        raise NotImplementedError

    def _run_todo_list(self, data_source, current_count):
        self._logger.debug('Begin _run_todo_list')
        result = 0
        for entry in self._entries:
            do_one_ray(entry, self._organizer)
        # organizer_ref = ray.put(self._organizer)
        # entry_references = [ray.put(entry) for entry in self._entries]
        # execution_references = [do_one_ray.remote(entry_ref, organizer_ref) for entry_ref in entry_references]
        # for reference in execution_references:
        #     # call ray.get as late as possible
        #     result |= ray.get(reference)
        self._logger.debug('End _run_todo_list')
        return result


class StateRunnerNoRemoteReaderNoDataSource(TodoRunner):
    """This Runner will stage data from a remote location, and then execute ingestion from the staging location."""

    def __init__(self, config, organizer, reporter):
        super().__init__(
            config=config,
            organizer=organizer,
            builder=None,
            data_sources=None,
            metadata_reader=None,
            reporter=reporter,
        )

    def run(self):
        """
        :return: 0 for success, -1 for failure
        """
        self._logger.debug('Begin run')
        state = StateRay()
        state.read_from_file(self._config.state_fqn)
        for data_source in self._config.data_sources:
            self._logger.info(f'Begin run for data source {data_source}')
            start_dt = state.get_bookmark_start(data_source)
            end_dt = state.get_bookmark_end(data_source)

            prev_exec_time = start_dt
            incremented = increment_time(prev_exec_time, self._config.interval)
            exec_time = min(incremented, end_dt)

            self._logger.info(f'Starting at {prev_exec_time}, ending at {end_dt}')
            result = 0
            if prev_exec_time == end_dt:
                self._logger.info(f'Start time is the same as end time {prev_exec_time}, stopping.')
                exec_time = prev_exec_time
            else:
                cumulative = 0
                result = 0
                while exec_time <= end_dt:
                    self._logger.info(f'Processing {data_source} from {prev_exec_time} to {exec_time}')
                    save_time = exec_time
                    runner = RayTodoRunner(
                        self._config, self._organizer, self._reporter, prev_exec_time, exec_time, data_source
                    )
                    runner.run()
                    # self._record_progress(num_entries, cumulative, prev_exec_time, save_time)
                    cumulative += self._record_progress(runner, cumulative, prev_exec_time, save_time)
                    state.save_start_dt(data_source, save_time, self._config.state_fqn)

                    if exec_time == end_dt:
                        # the last interval will always have the exec time
                        # equal to the end time, which will fail the while check
                        # so leave after the last interval has been processed
                        #
                        # but the while <= check is required so that an interval
                        # smaller than exec_time -> end_time will get executed,
                        # so don't get rid of the '=' in the while loop
                        # comparison, just because this one exists
                        break
                    prev_exec_time = exec_time
                    new_time = increment_time(prev_exec_time, self._config.interval)
                    exec_time = min(new_time, end_dt)
                    # TODO the real time to store is going to be the runner.max_dt - how to find that,
                    # how to reference it, etc

            state.save_start_dt(data_source, exec_time, self._config.state_fqn)
            self._end_message(data_source, exec_time)
            self._logger.debug(f'End run for data source {data_source} with result {result}')
        self._logger.debug('End run')
        return result

    def _end_message(self, data_source, exec_time):
        msg = f'Done for {data_source}, saved state is {exec_time}'
        self._logger.info('=' * len(msg))
        self._logger.info(msg)
        self._logger.info(f'{self._reporter.success} of {self._reporter.all} records processed correctly.')
        self._logger.info('=' * len(msg))

    def _record_progress(self, runner, cumulative, start_time, save_time):
        with open(self._config.progress_fqn, 'a') as progress:
            progress.write(
                f'{datetime.now()} current:: {save_time} {runner.num_entries} since:: {start_time}:: {cumulative}\n'
            )
        return runner.num_entries


def ray_execution(data_visitors, meta_visitors):
    # ray.init()
    config = Config()
    config.get_executors()
    reporter = ExecutionReporterRay(config)
    organizer = OrganizeExecutesRay(config, data_visitors, meta_visitors, reporter)
    runner = StateRunnerNoRemoteReaderNoDataSource(
        config=config,
        organizer=organizer,
        reporter=reporter,
    )
    return runner.run()
