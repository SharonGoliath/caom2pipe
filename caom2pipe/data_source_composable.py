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
import os
import shutil
import traceback

from collections import deque, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone

from cadctap import CadcTapClient
from cadcutils import exceptions
from caom2pipe import astro_composable as ac
from caom2pipe import client_composable as clc
from caom2pipe import manage_composable as mc
from caom2pipe import name_builder_composable as nbc

__all__ = [
    'DataSource',
    'data_source_factory',
    'IncrementalDataSource',
    'is_offset_aware',
    'ListDirDataSource',
    'ListDirSeparateDataSource',
    'ListDirTimeBoxDataSource',
    'LocalFilesDataSource',
    'QueryTimeBoxDataSource',
    'RetryTodoFileDataSource',
    'StateRunnerMeta',
    'TodoFileDataSource',
    'VaultCleanupDataSource',
    'VaultDataSource',
]


class DataSource:
    """
    The mechanisms for identifying the lists of work to be done (otherwise
    known as which class instantiates the extension of the DataSource class)
    are:
    - a todo.txt file, where every entry in the file is a unit of work
    - a directory listing (i.e. ls('./dir/*.fz')), where every file listed
        is a unit of work
    - a time-boxed listing (based on bookmarks in a state.yml file) - an
        incremental approach, where the work to be done is chunked (by
        time-boxes for now, as it's the only in-use requirement)
    - a time-boxed directory listing
    - an implementation of the work_composable.work class, which returns a
        deque of work todo as a method implementation
    """

    def __init__(self, config):
        """
        :param config:
        """
        self._config = config
        self._extensions = config.data_source_extensions
        self._rejected_files = 0
        self._skipped_files = 0
        self._work = deque()
        self._reporter = None
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def reporter(self):
        return self._reporter

    @reporter.setter
    def reporter(self, value):
        self._reporter = value

    def clean_up(self, entry, execution_result, current_count):
        """Clean up files locally after there has been a storage attempt."""
        pass

    def default_filter(self, entry):
        """
        :param entry: os.DirEntry
        """
        for extension in self._extensions:
            if entry.name.endswith(extension):
                return True
        return False

    def get_work(self):
        return self._work

    def _capture_todo(self):
        self._reporter.capture_todo(len(self._work), self._rejected_files, self._skipped_files)
        # do not need the record of the rejected or skipped files any longer
        self._rejected_files = 0
        self._skipped_files = 0


class IncrementalDataSource(DataSource):
    """
    Support incremental harvesting.

    On "end time" for incremental execution:
    - the general rule is it should be the maximum time of the last record to be processed, as obtained from the data
      source
    - it should be over-ride-able, for the cases of:
        - local directory listing time-boxes, where additions/changes to the timestamps in the list will be reflected
          in the known system time
        - horizontal scaling when reading from a data source
    - if start_dt == end_dt for an incremental run, the initialize_end_dt may find candidates for processing, but the
      run method in run_composable.StateRunner will skip doing those candidates again, as the condition
      start_dt == end_dt exits without doing any work.
    - for a data source, setting the default start_dt to the initial time for data from the source will result in a
      a complete harvest, such as is required for validation.

    The pipelines will follow the guidance of "naive as local" from this reference:
    https://blog.ganssle.io/articles/2022/04/naive-local-datetimes.html. The takeaways are repeated here:
    - the offset may change during an interpreter run
    - use datetime.astimezone(None) to convert to an aware datetime with a fixed offset representing the current
      local time
    - arithmetic operations should be applied to naive datetimes when working in local time

    Effects on the pipeline:
    - wall time arithmetic will be used on the values stored to and from the state.yml file;
      (naive_dt + timedelta(hours=offset)).astimezone()
    """

    def __init__(self, config, start_key):
        """
        Do what needs to be done to set end_dt for an incremental harvest.

        Use the method initialize_end_dt if it's necessary to know the "end time" for an incremental harvest,
        before incremental execution starts

        :param config: manage_composable.Config instance
        :param start_key: key for looking up bookmarks in state.yml
        """
        super().__init__(config)
        self._start_key = start_key
        # do not initialize this here, so that a DataSource may also be used in a TodoRunner circumstance, without
        # with no need for a state.yml file on disk
        self._state = None
        # start_dt and end_dt values should be naive datetimes
        self._start_dt = None
        self._end_dt = None

    def __str__(self):
        return f'\nstart: {self._start_dt}' \
               f'\n  end: {self._end_dt}' \
               f'\n  key: {self._start_key}'

    @property
    def end_dt(self):
        return self._end_dt

    @end_dt.setter
    def end_dt(self, value):
        self._end_dt = value

    @property
    def start_dt(self):
        return self._start_dt

    @start_dt.setter
    def start_dt(self, value):
        self._start_dt = value

    @property
    def start_key(self):
        return self._start_key

    def get_time_box_work(self, prev_exec_dt, exec_dt):
        """
        Time-boxing the file url list returned from the site scrape.

        :param prev_exec_dt tz-naive datetime start of the time-box chunk
        :param exec_dt tz-naive datetime end of the time-box chunk
        :return: a deque of StateRunnerMeta instances, for file names with
            time they were modified
        """
        self._logger.debug(f'Begin get_time_box_work from {prev_exec_dt} to {exec_dt} for {self._start_key}')

        temp = deque()
        for entry in self._work:
            if prev_exec_dt < entry.entry_dt <= exec_dt:
                temp.append(entry)
        self._reporter.capture_todo(len(temp), self._rejected_files, self._skipped_files)
        self._rejected_files = 0
        self._skipped_files = 0
        self._logger.debug(f'End get_time_box_work with {len(temp)} entries.')
        return temp

    def initialize_end_dt(self):
        """
        Do what needs to be done to set end_dt for an incremental harvest.
        """
        self._logger.debug('Begin initialize_end_dt')
        end_timestamp = self._state.bookmarks.get(self._start_key).get('end_timestamp')
        if end_timestamp is None:
            self._initialize_end_dt()
        else:
            self._end_dt = mc.make_datetime(end_timestamp)
        self._logger.debug(f'End initialize_end_dt with {self._end_dt}')

    def initialize_start_dt(self):
        self._logger.debug('Begin initialize_start_dt')
        self._state = mc.State(self._config.state_fqn, self._config.time_zone)
        self._start_dt = self._state.get_bookmark(self._start_key)
        self._logger.debug(f'End initialize_start_dt with {self._start_dt}')

    def save_start_dt(self, value):
        self._state.save_state(self._start_key, value)

    def _initialize_end_dt(self):
        raise NotImplementedError


class ListDirDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing. This is the original use_local_files: True behaviour.
    """

    def __init__(self, config, chooser):
        super().__init__(config)
        self._chooser = chooser

    def get_work(self):
        self._logger.debug(f'Begin get_work from {self._config.working_directory}.')
        file_list = os.listdir(self._config.working_directory)
        work = deque()
        for f in file_list:
            f_name = None
            if f.endswith('.fits') or f.endswith('.fits.gz'):
                if self._chooser is None:
                    f_name = f
                else:
                    if self._chooser.use_compressed(f):
                        if f.endswith('.fits'):
                            f_name = f'{f}.gz'
                        else:
                            f_name = f
                    else:
                        if f.endswith('.fits.gz'):
                            f_name = f.replace('.gz', '')
                        else:
                            f_name = f
            elif f.endswith('.header'):
                f_name = f
            elif f.endswith('.fz'):
                f_name = f
            elif f.endswith('.hdf5') or f.endswith('.h5'):
                f_name = f
            elif f.endswith('.json'):
                f_name = f
            elif f.endswith('.cat'):
                # NGVS
                f_name = f
            elif f.endswith('.mask.rd.reg'):
                # NGVS
                f_name = f
            if f_name is not None:
                self._logger.debug(f'{f_name} added to work list.')
                work.append(f_name)
        # ensure unique entries
        self._work = deque(set(work))
        self._capture_todo()
        self._logger.debug(f'End get_work.')
        return self._work


class ListDirSeparateDataSource(DataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing for a collection of directories.

    This specialization is meant to imitate the behaviour of the original
    "ListDirDataSource", with different assumptions about which directories
    to use as a source of files, and how to identify files of interest.

    This specialization makes minimalistic checking choices so large file sets aren't unbearably slow.
    """

    def __init__(self, config):
        super().__init__(config)
        self._source_directories = config.data_sources
        self._extensions = config.data_source_extensions
        self._recursive = config.recurse_data_sources
        self._work = deque()

    def default_filter(self, entry):
        work_with_file = False
        for extension in self._extensions:
            if entry.name.endswith(extension):
                work_with_file = True
                break
        return work_with_file

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        for source in self._source_directories:
            self._logger.info(f'Look in {source} for work.')
            self._append_work(source)
        self._logger.debug('End get_work')
        self._capture_todo()
        return self._work

    def _append_work(self, entry):
        with os.scandir(entry) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    self._append_work(entry.path)
                else:
                    if self.default_filter(entry):
                        self._logger.debug(f'Adding {entry.path} to work list.')
                        self._work.append(entry.path)


class ListDirTimeBoxDataSource(IncrementalDataSource):
    """
    A time-boxed directory listing of all .fits* and .hdf5 files. The time-box
    is based on os.stat.st_mtime for a file.

    Implementation choices:
    - one or many directories? one is fire-and-forget, many is probably
      more accurate of operational conditions - e.g. VLASS1.1, VLASS1.2,
      etc, OR vos:Nat/NIFS/...., OR NEOSSAT's NESS, 2017, 2018, 2019, etc
    - send it off to glob, walk, or do it semi-here? one/a few high-level
      directories that point to a lot of occupied storage space could take a
      very long time, and a lot of memory, in glob/walk
    """

    def __init__(self, config):
        super().__init__(config, config.bookmark)
        self._source_directories = config.data_sources
        self._recursive = config.recurse_data_sources
        self._temp = defaultdict(list)

    def _initialize_end_dt(self):
        self._end_dt = mc.get_now()

    def get_time_box_work(self, prev_exec_dt, exec_dt):
        """
        :param prev_exec_dt tz-naive datetime start of the time-boxed chunk
        :param exec_dt tz-naive datetime end of the time-goxed chunk
        :return: a deque of StateRunnerMeta instances, with
            prev_exec_time <= os.stat.mtime <= exec_time, and sorted by
            os.stat.mtime
        """
        self._logger.debug(
            f'Begin get_time_box_work from {prev_exec_dt} to {exec_dt}.'
        )
        for source in self._source_directories:
            self._logger.debug(f'Looking for work in {source}')
            self._append_work(prev_exec_dt, exec_dt, source)
        # ensure the result returned is sorted by timestamp in ascending
        # order
        for mtime in sorted(self._temp):
            for entry in self._temp[mtime]:
                self._work.append(StateRunnerMeta(entry_name=entry, entry_dt=mtime))
        self._temp = defaultdict(list)
        self._capture_todo()
        self._logger.debug('End get_time_box_work')
        return self._work

    def _append_work(self, prev_exec_dt, exec_dt, entry_path):
        with os.scandir(entry_path) as dir_listing:
            for entry in dir_listing:
                # the slowest thing to do is the 'stat' call, so delay it as
                # long as possible, and only if necessary
                if entry.is_dir() and self._recursive:
                    entry_stats = entry.stat()
                    entry_st_mtime_dt = mc.make_datetime(entry_stats.st_mtime)
                    if exec_dt >= entry_st_mtime_dt >= prev_exec_dt:
                        self._append_work(prev_exec_dt, exec_dt, entry.path)
                else:
                    # send the dir_listing value
                    if self.default_filter(entry):
                        entry_stats = entry.stat()
                        entry_st_mtime_dt = mc.make_datetime(entry_stats.st_mtime)
                        if exec_dt >= entry_st_mtime_dt >= prev_exec_dt:
                            self._temp[entry_st_mtime_dt].append(entry.path)
                            self._logger.debug(f'Adding {entry.path}')


class LocalFilesDataSource(ListDirTimeBoxDataSource):
    """
    For when use_local_files: True and cleanup_when_storing: True
    """

    def __init__(self, config, cadc_client, metadata_reader, recursive=True, scheme='cadc'):
        super().__init__(config)
        self._retry_failures = config.retry_failures
        self._retry_count = config.retry_count
        self._cadc_client = cadc_client
        self._cleanup_when_storing = config.cleanup_files_when_storing
        self._cleanup_failure_directory = config.cleanup_failure_destination
        self._cleanup_success_directory = config.cleanup_success_destination
        self._store_modified_files_only = config.store_modified_files_only
        self._source_directories = config.data_sources
        self._collection = config.collection
        self._recursive = recursive
        self._metadata_reader = metadata_reader
        self._metadata_reader.working_directory = config.working_directory
        self._is_connected = config.is_connected
        self._scheme = scheme
        if not self._is_connected:
            # assume iterative testing is the objective for SCRAPE'ing,
            # and over-ride the configuration that will undermine that
            # behaviour.
            self._cleanup_when_storing = False
            self._logger.info(
                'SCRAPE\'ing data - over-riding config.yml '
                'cleanup_files_when_storing setting.'
            )
        if mc.TaskType.STORE not in config.task_types:
            # do not clean up files unless the STORE task is configured
            self._cleanup_when_storing = False
            self._logger.info(
                'Not STORE\'ing data - ignore config.yml '
                'cleanup_files_when_storing setting.'
            )

    def get_collection(self, ignore=None):
        return self._collection

    def clean_up(self, entry, execution_result, current_count=0):
        """
        Move a file to the success or failure location, depending on
        whether a file with the same checksum is at CADC.

        Move the file only after all retries (as specified in config.yml)
        have been attempted. Note that this cleanup behaviour will be assigned
        to the TodoFileDataSource during a retry.

        :param entry: either a data_source_composable.StateRunnerMeta instance
            or an str, depending on whether the clean-up is invoked from a
            time-boxed or all-in-one invocation.
        :param execution_result: int if it's 0, it's ok to clean up,
            regardless of how many times a file has been processed
        :param current_count: int how many retries have been executed
        """
        self._logger.debug(f'Begin clean_up with {entry}')
        if self._cleanup_when_storing and (
            (not self._retry_failures)
            or (self._retry_failures and current_count >= self._retry_count)
            or (self._retry_failures and execution_result == 0)
        ):
            if isinstance(entry, str):
                fqn = entry
            else:
                fqn = entry.entry_name
            self._logger.debug(f'Clean up {fqn}')
            if self._is_remote_different(fqn):
                # the transfer itself failed, so track as a failure
                self._move_action(fqn, self._cleanup_failure_directory)
            else:
                self._move_action(fqn, self._cleanup_success_directory)
        self._logger.debug('End clean_up.')

    def _verify_file(self, fqn):
        """
        Check file content for correctness, by whatever rules the file needs to conform to. The default
        implementation is for FITS files only.

        :param fqn: str fully-qualified file name
        :return: True if the file passes the check, False otherwise
        """
        return ac.check_fitsverify(fqn)

    def default_filter(self, entry):
        """
        :param entry: os.DirEntry
        """
        work_with_file = True
        if super().default_filter(entry):
            if entry.name.startswith('.'):
                # skip dot files
                work_with_file = False
            elif '.hdf5' in entry.name:
                # no hdf5 validation
                pass
            elif self._verify_file(entry.path):
                # only work with files that pass the FITS verification
                if self._cleanup_when_storing:
                    if self._store_modified_files_only:
                        # only transfer files with a different MD5 checksum
                        work_with_file = self._is_remote_different(entry.path)
                        if not work_with_file:
                            self._logger.warning(
                                f'{entry.path} has the same md5sum at CADC. '
                                f'Not transferring.'
                            )
                            # KW - 23-06-21
                            # if the file already exists, with the same
                            # checksum, at CADC, Kanoa says move it to the
                            # 'succeeded' directory.
                            self._skipped_files += 1
                            self._move_action(entry.path, self._cleanup_success_directory)
                            self._reporter.capture_success(entry.name, entry.name, datetime.now(tz=timezone.utc).timestamp())
                else:
                    work_with_file = True
            else:
                self._rejected_files += 1
                if self._cleanup_when_storing:
                    self._logger.warning(f'Rejecting {entry.path}. Moving to {self._cleanup_failure_directory}')
                    self._move_action(entry.path, self._cleanup_failure_directory)
                temp_storage_name = mc.StorageName(file_name=entry.name)
                self._reporter.capture_failure(
                    temp_storage_name, BaseException('_verify_file errors'), '_verify_file errors'
                )
                work_with_file = False
        else:
            work_with_file = False
        self._logger.debug(f'Done default_filter says work_with_file is {work_with_file} for {entry.name}')
        return work_with_file

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        for source in self._source_directories:
            self._logger.info(f'Look in {source} for work.')
            self._find_work(source)
        self._logger.debug('End get_work')
        self._capture_todo()
        return self._work

    def _append_work(self, prev_exec_dt, exec_dt, entry_path):
        with os.scandir(entry_path) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    entry_stats = entry.stat()
                    entry_st_mtime_dt = datetime.fromtimestamp(entry_stats.st_mtime)
                    if exec_dt >= entry_st_mtime_dt >= prev_exec_dt:
                        self._append_work(prev_exec_dt, exec_dt, entry.path)
                else:
                    # order the stats check before the default_filter check,
                    # because CFHT likes to work with tens of thousands of
                    # files, not a few, and the default filter is the one
                    # that opens and reads every file to see if it's a valid
                    # FITS file
                    #
                    # send the dir_listing value
                    # skip dot files, but have a special exclusion, because
                    # otherwise the entry.stat() call will sometimes fail.
                    if not entry.name.startswith('.'):
                        entry_stats = entry.stat()
                        entry_st_mtime_dt = datetime.fromtimestamp(entry_stats.st_mtime)
                        if exec_dt >= entry_st_mtime_dt >= prev_exec_dt:
                            if self.default_filter(entry):
                                self._temp[entry_st_mtime_dt].append(entry.path)

    def _is_remote_different(self, entry_path):
        """
        Check whether a file is different at its source than it is at CADC, using md5sum comparisons.

        :return: boolean False if the data is the same locally as at CADC, True otherwise
        """
        # get the metadata locally
        result = True
        if self._is_connected:
            # get the CADC FileInfo
            f_name = os.path.basename(entry_path)
            destination_name = mc.build_uri(self.get_collection(f_name), f_name, self._scheme)
            try:
                cadc_meta = self._cadc_client.info(destination_name)
            except Exception as e:
                self._logger.error(
                    f'info call failed for {destination_name} with {e}'
                )
                self._logger.debug(traceback.format_exc())
                cadc_meta = None

            if cadc_meta is None:
                result = True
            else:
                # get the local FileInfo
                temp_storage_name = mc.StorageName(
                    file_name=f_name, source_names=[entry_path]
                )
                temp_storage_name._destination_uris = [destination_name]
                self._metadata_reader.set_file_info(temp_storage_name)
                if (
                    self._metadata_reader.file_info.get(destination_name).md5sum.replace('md5:', '')
                    == cadc_meta.md5sum.replace('md5:', '')
                ):
                    result = False
        else:
            self._logger.debug(
                f'SCRAPE\'ing data - no md5sum checking with CADC for '
                f'{entry_path}.'
            )
        temp_text = 'different' if result else 'same'
        self._logger.debug(
            f'Done _is_remote_different for {entry_path} result is {temp_text} at '
            f'CADC.'
        )
        return result

    def _find_work(self, entry_path):
        with os.scandir(entry_path) as dir_listing:
            for entry in dir_listing:
                if entry.is_dir() and self._recursive:
                    self._find_work(entry.path)
                else:
                    if self.default_filter(entry):
                        self._logger.info(
                            f'Adding {entry.path} to work list.'
                        )
                        self._work.append(entry.path)

    def _move_action(self, fqn, destination):
        # if move when storing is enabled, move to an after-action location
        if self._cleanup_when_storing:
            # shutil.move is atomic if it's within a file system, which I
            # believe is the description Kanoa gave. It also supports
            # the same behaviour as
            # https://www.gnu.org/software/coreutils/manual/html_node/
            # mv-invocation.html#mv-invocation when copying between
            # file systems for moving a single file.
            try:
                f_name = os.path.basename(fqn)
                # if the destination is a fully-qualified name, an
                # over-write will succeed
                dest_fqn = os.path.join(destination, f_name)
                self._logger.debug(f'Moving {fqn} to {dest_fqn}')
                shutil.move(fqn, dest_fqn)
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                self._logger.error(f'Failed to move {fqn} to {destination}')
                raise mc.CadcException(e)


class TodoFileDataSource(DataSource):
    """
    Implements the identification of the work to be done, by reading the
    contents of a file.
    """

    def __init__(self, config):
        super().__init__(config)

    def get_work(self):
        self._logger.debug(f'Begin get_work from {self._config.work_fqn}.')
        self._work = deque()
        with open(self._config.work_fqn) as f:
            for line in f:
                temp = line.strip()
                if len(temp) > 0:
                    # ignore empty lines
                    self._logger.debug(f'Adding entry {temp} to work list.')
                    self._work.append(temp)
        self._capture_todo()
        self._logger.debug(f'End get_work.')
        return self._work


class RetryTodoFileDataSource(TodoFileDataSource):
    """
    Extends the TodoFileDataSource to not update the "Number of Inputs" report line when retrying ingestion.
    """

    def __init__(self, config):
        super().__init__(config)

    def _capture_todo(self):
        pass


def is_offset_aware(dt):
    """
    :return: return True if tzinfo is set
    """
    return False if dt.tzinfo is None else True


@dataclass
class StateRunnerMeta:
    # how to refer to the item of work to be processed
    entry_name: str
    # offset-aware datetime associated with item of work
    entry_dt: datetime


class QueryTimeBoxDataSource(IncrementalDataSource):
    """
    Implements the identification of the work to be done, by querying a
    TAP service, in time-boxed chunks. The time values are timestamps
    (floats).

    Deprecate the QueryTimeBoxDataSource class in favour of this
    implementation.
    """

    def __init__(self, config, preview_suffix='jpg'):
        super().__init__(config, config.bookmark)
        self._preview_suffix = preview_suffix
        subject = clc.define_subject(config)
        self._client = CadcTapClient(subject, resource_id=self._config.storage_inventory_tap_resource_id)

    def get_time_box_work(self, prev_exec_dt, exec_dt):
        """
        Get a set of file names from a collection. Limit the entries by
        time-boxing on lastModified, and don't include previews.

        :param prev_exec_dt tz-naive datetime start of the time-boxed chunk
        :param exec_dt tz-naive datetime end of the time-boxed chunk
        :return: a list of StateRunnerMeta instances in the CADC storage system
        """
        # SG 8-09-22
        # All timestamps in the SI databases are in UTC
        #
        # SGo
        # Use 'lastModified', because that should be the later timestamp (avoid eventual consistency lags).
        self._logger.debug(f'Begin get_time_box_work.')
        prev_exec_dt = mc.make_datetime(prev_exec_dt)
        exec_dt = mc.make_datetime(exec_dt)
        query = f"""
            SELECT A.uri, A.lastModified
            FROM inventory.Artifact AS A
            WHERE A.uri NOT LIKE '%{self._preview_suffix}'
            AND A.lastModified > '{prev_exec_dt}'
            AND A.lastModified <= '{exec_dt}'
            AND split_part( split_part( A.uri, '/', 1 ), ':', 2 ) = '{self._config.collection}'
            ORDER BY A.lastModified ASC
        """
        self._logger.debug(query)
        rows = clc.query_tap_client(query, self._client)
        self._work = deque()
        for row in rows:
            ignore_scheme, ignore_path, f_name = mc.decompose_uri(row['uri'])
            r_dt = mc.make_datetime(row['lastModified'])
            self._work.append(StateRunnerMeta(f_name, r_dt))
        self._capture_todo()
        self._logger.debug(f'End get_time_box_work.')
        return self._work

    def _initialize_end_dt(self):
        query = f"""
            SELECT max(A.lastModified) AS m
            FROM inventory.Artifact AS A
            WHERE A.uri NOT LIKE '%{self._preview_suffix}'
            AND split_part( split_part( A.uri, '/', 1 ), ':', 2 ) = '{self._config.collection}'
        """
        self._logger.debug(query)
        rows = clc.query_tap_client(query, self._client)
        for row in rows:
            self._end_dt = mc.make_datetime(row['m'])
            self._logger.info(f'Setting end time to {self._end_dt}')
            break


class VaultDataSource(ListDirTimeBoxDataSource):
    """
    Implement the identification of the work to be done, by doing a directory
    listing on VOSpace.

    The directory listing may be time-boxed.
    """

    def __init__(self, vault_client, config):
        super().__init__(config)
        self._vault_client = vault_client

    def get_work(self):
        self._logger.debug('Begin get_work.')
        self._work = deque()
        for source_directory in self._source_directories:
            self._logger.debug(
                f'Searching {source_directory} for work to do.'
            )
            self._find_work(source_directory, self._work)
        self._capture_todo()
        self._logger.debug('End get_work.')
        return self._work

    def _append_work(self, prev_exec_dt, exec_dt, entry):
        """
        :param prev_exec_dt: datetime
        :param exec_dt: datetime
        :param entry: Vault URI
        """
        self._logger.info(f'Begin _append_work in {entry}.')
        # force = True means do not use the cache
        node = self._vault_client.get_node(entry, limit=None, force=True)
        while node.type == 'vos:LinkNode':
            uri = node.target
            node = self._vault_client.get_node(uri, limit=None, force=True)
        for target in node.node_list:
            target_fqn = f'{node.uri}/{target}'
            target_node = self._vault_client.get_node(target_fqn)
            target_node_mtime = mc.make_datetime(target_node.props.get('date'))
            if target_node.type == 'vos:ContainerNode' and self._recursive:
                if exec_dt >= target_node_mtime >= prev_exec_dt:
                    self._append_work(
                        prev_exec_dt, exec_dt, target_node.uri
                    )
            else:
                if exec_dt >= target_node_mtime >= prev_exec_dt:
                    if self.default_filter(target_node):
                        self._temp[target_node_mtime].append(target_node.uri)
                        self._logger.info(
                            f'Add {target_node.uri} to work list.'
                        )
        self._logger.debug('End _append_work')

    def _find_work(self, source_directory, work):
        node = self._vault_client.get_node(
            source_directory, limit=None, force=True
        )
        while node.type == 'vos:LinkNode':
            uri = node.target
            node = self._vault_client.get_node(uri, limit=None, force=True)
        for entry in node.node_list:
            if entry.type == 'vos:ContainerNode' and self._recursive:
                self._find_work(entry.uri, work)
            else:
                if self.default_filter(entry):
                    self._logger.info(f'Add {entry.uri} to work list.')
                    work.append(entry.uri)

    def default_filter(self, target_node):
        """
        :param target_node: Node
        """
        # make a Node look like an os.DirEntry, which is expected by
        # the super invocation
        dir_entry = type('', (), {})
        dir_entry.name = os.path.basename(target_node.uri)
        dir_entry.path = target_node.uri
        if super().default_filter(dir_entry):
            target_node_size = target_node.props.get('size')
            if target_node_size == 0:
                self._logger.info(f'Skipping 0-length {target_node.uri}')
            else:
                return True
        return False


class VaultCleanupDataSource(VaultDataSource):
    """
    This implementation will clean up a vos: data source.
    """

    def __init__(self, config, vault_client, cadc_client, metadata_reader, storage_name_ctor=mc.StorageName):
        super().__init__(vault_client, config)
        self._cleanup_when_storing = config.cleanup_files_when_storing
        self._cleanup_failure_directory = config.cleanup_failure_destination
        self._cleanup_success_directory = config.cleanup_success_destination
        self._retry_failures = config.retry_failures
        self._retry_count = config.retry_count
        self._store_modified_files_only = config.store_modified_files_only
        self._collection = config.collection
        self._recursive = config.recurse_data_sources
        self._cadc_client = cadc_client
        self._scheme = config.scheme
        self._metadata_reader = metadata_reader
        if mc.TaskType.STORE not in config.task_types:
            # do not clean up files unless the STORE task is configured
            self._cleanup_when_storing = False
            self._logger.info('Not STORE\'ing data - ignore config.yml cleanup_files_when_storing setting.')
        self._storage_name_ctor = storage_name_ctor

    def get_collection(self, f_name):
        return self._collection

    def clean_up(self, entry, execution_result, current_count):
        """
        Move a file to the success or failure location, depending on whether
        a file with the same checksum is at CADC.
        """
        self._logger.debug(f'Begin clean_up with {entry}')
        if self._cleanup_when_storing and (
            (not self._retry_failures)
            or (self._retry_failures and current_count >= self._retry_count)
            or (self._retry_failures and execution_result == 0)
        ):
            if isinstance(entry, str):
                fqn = entry
            else:
                fqn = entry.entry_name
            storage_name = nbc.GuessingBuilder(self._storage_name_ctor).build(fqn)
            self._logger.debug(f'Clean up {fqn}')
            if storage_name.destination_uris[0] in self._metadata_reader.file_info:
                if self._is_remote_different(storage_name.destination_uris[0]):
                    # the transfer itself failed, so track as a failure
                    self._move_action(fqn, self._cleanup_failure_directory)
                else:
                    self._move_action(fqn, self._cleanup_success_directory)
            else:
                self._logger.warning(f'No clean up for {fqn} because there is no vos metadata.')
            self._logger.debug('End clean_up.')

    def default_filter(self, dir_entry):
        """
        :param dir_entry: either an str that is a vos URI, or a vos.Node
        """
        dir_entry_fqn = dir_entry
        if not isinstance(dir_entry, str):
            dir_entry_fqn = dir_entry.uri
        self._logger.debug(f'Begin default_filter with {dir_entry_fqn}')
        copy_file = False
        for extension in self._extensions:
            if dir_entry_fqn.endswith(extension):
                copy_file = True
                if os.path.basename(dir_entry_fqn).startswith('.'):
                    # skip dot files
                    copy_file = False
                else:
                    storage_name = nbc.GuessingBuilder(self._storage_name_ctor).build(dir_entry_fqn)
                    self._metadata_reader.set_file_info(storage_name)
                    if self._metadata_reader.file_info[storage_name.destination_uris[0]].size == 0:
                        self._logger.info(f'Skipping 0-length {dir_entry_fqn}')
                        copy_file = False
                    elif self._store_modified_files_only:
                        # only transfer files with a different MD5 checksum
                        copy_file = self._is_remote_different(storage_name.destination_uris[0])
                        if not copy_file:
                            self._skipped_files += 1
                            self._move_action(dir_entry_fqn, self._cleanup_success_directory)
                            self._reporter.capture_success(
                                dir_entry_fqn, os.path.basename(dir_entry_fqn), datetime.now(tz=timezone.utc).timestamp()
                            )
                break
        self._logger.debug(f'Done default_filter says copy_file is {copy_file} for {dir_entry_fqn}')
        return copy_file

    def get_work(self):
        self._logger.debug(f'Begin get_work.')
        for source in self._source_directories:
            self._logger.info(f'Look in {source} for work.')
            self._find_work(source)
        self._logger.debug('End get_work')
        self._capture_todo()
        return self._work

    def _is_remote_different(self, destination_uri):
        """
        :param destination_uri: str CADC storage system URI
        """
        self._logger.debug(f'Begin _is_remote_different {destination_uri}')
        result = True
        # get the metadata at CADC
        cadc_meta = self._cadc_client.info(destination_uri)
        if cadc_meta is not None and self._metadata_reader.file_info[destination_uri].md5sum == cadc_meta.md5sum:
            self._logger.warning(f'{destination_uri} has the same md5sum at CADC.')
            result = False
        return result

    def _find_work(self, entry):
        # dir_listing is a list of str
        dir_listing = self._vault_client.listdir(entry)
        for dir_entry in dir_listing:
            dir_entry_fqn = f'{entry}/{dir_entry}'
            if self._vault_client.isdir(dir_entry_fqn) and self._recursive:
                self._find_work(dir_entry_fqn)
            else:
                if self.default_filter(dir_entry_fqn):
                    self._logger.info(f'Adding {dir_entry_fqn} to work list.')
                    self._work.append(dir_entry_fqn)

    def _move_action(self, fqn, destination):
        """
        :param fqn: VOS URI, includes a file name
        :param destination: VOS URI, points to a directory
        :return:
        """
        # if move when storing is enabled, move to an after-action location
        self._logger.debug(f'Begin _move_action for {fqn}')
        if self._cleanup_when_storing:
            self._logger.debug(f'Carry out _move_action for {fqn}')
            try:
                f_name = os.path.basename(fqn)
                dest_fqn = os.path.join(destination, f_name)
                try:
                    if self._vault_client.status(dest_fqn):
                        # vos: doesn't support over-write
                        self._logger.warning(
                            f'Removing {dest_fqn} prior to over-write.'
                        )
                        self._vault_client.delete(dest_fqn)
                except exceptions.NotFoundException as not_found_e:
                    # do nothing, since the node doesn't exist
                    pass
                self._logger.warning(f'Moving {fqn} to {dest_fqn}')
                self._vault_client.move(fqn, dest_fqn)
            except Exception as e:
                self._logger.debug(traceback.format_exc())
                self._logger.error(f'Failed to move {fqn} to {destination}')
                raise mc.CadcException(e)
        self._logger.debug('End _move_action')


def data_source_factory(config, clients, state, reader, reporter):
    """
    :param config: manage_composable.Config
    :param clients: client_composable.ClientCollection
    :param state: bool True is the DataSource is time-boxed.
    :param reporter: ExecutionReporter instance tracks and reports on successes and failures
    :return: DataSource specialization
    """
    if config.use_local_files:
        # by default call fitsverify as part of setting up local file use
        source = LocalFilesDataSource(config, clients.data_client, reader, config.recurse_data_sources, config.scheme)
    else:
        if config.use_vos and clients.vo_client is not None:
            if config.cleanup_files_when_storing:
                source = VaultCleanupDataSource(config, clients.vo_client, clients.data_client, reader)
            else:
                source = VaultDataSource(clients.vo_client, config)
        else:
            if state:
                source = QueryTimeBoxDataSource(config)
            else:
                source = TodoFileDataSource(config)
    source.reporter = reporter
    logging.debug(f'Created {source.__class__.__name__} data_source_composable instance.')
    return source
