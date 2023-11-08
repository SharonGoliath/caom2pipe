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

import csv
import importlib
import io
import logging
import os
import re
import stat

import requests
import subprocess
import sys
import traceback
import yaml

from dataclasses import dataclass
from datetime import datetime, timedelta
from dateutil import parser, tz
from enum import Enum
from hashlib import md5
from importlib_metadata import version, PackageNotFoundError
from io import BytesIO
from requests.adapters import HTTPAdapter
from shutil import move
from urllib import parse as parse
from urllib3 import Retry

from astropy.table import Table

from cadcutils import net
from cadcdata import CadcDataClient
from cadctap import CadcTapClient
from caom2 import ObservationWriter, ObservationReader, Artifact, Observation
from caom2 import ChecksumURI, ProductType, ReleaseType
from caom2.diff import get_differences


__all__ = [
    'build_uri',
    'Cache',
    'CadcException',

    'CaomName',
    'compare_observations',
    'Config',
    'check_param',
    'convert_to_days',
    'exec_cmd',
    'exec_cmd_info',
    'exec_cmd_redirect',
    'ExecutionReporter',
    'extract_file_name_from_uri',
    'Features',
    'FileMeta',
    'ftp_get',
    'ftp_get_timeout',
    'get_artifact_metadata',
    'get_cadc_headers',
    'get_endpoint_session',
    'get_file_meta',
    'get_keyword',
    'get_now',
    'get_version',
    'http_get',
    'increment_time',
    'ISO_8601_FORMAT',
    'load_module',
    'make_datetime',
    'Metrics',
    'minimize_on_keyword',
    'Observable',
    'query_endpoint',
    'query_endpoint_session',
    'read_as_yaml',
    'read_csv_file',
    'read_from_file',
    'read_obs_from_file',
    'Rejected',
    'reverse_lookup',
    'StorageName',
    'State',
    'TaskType',
    'to_float',
    'to_int',
    'to_str',
    'update_typed_set',
    'ValueRepairCache',
    'write_obs_to_file',
    'write_to_file',
]

ISO_8601_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
READ_BLOCK_SIZE = 8 * 1024


class CadcException(Exception):
    """Generic exception raised by failure cases within the caom2pipe
    module."""

    pass


class Features:
    """Boolean feature flag implementation."""

    def __init__(self):
        self._run_in_airflow = True
        self._supports_composite = True
        self._supports_catalog = True
        self._supports_multiple_files = True

    @property
    def run_in_airflow(self):
        """If true, will treat command-line arguments as if the application
        is running in airflow."""
        return self._run_in_airflow

    @run_in_airflow.setter
    def run_in_airflow(self, value):
        self._run_in_airflow = value

    @property
    def supports_composite(self):
        """If true, will execute any specific code for composite observation
        definition."""
        return self._supports_composite

    @supports_composite.setter
    def supports_composite(self, value):
        self._supports_composite = value

    @property
    def supports_catalog(self):
        """If true, will execute any specific code for catalog handling
        when creating a CAOM instance."""
        return self._supports_catalog

    @supports_catalog.setter
    def supports_catalog(self, value):
        self._supports_catalog = value

    @property
    def supports_multiple_files(self):
        """If true, will execute any specific code where the cardinality
        between metadata and files is 1:n."""
        return self._supports_multiple_files

    @supports_multiple_files.setter
    def supports_multiple_files(self, value):
        self._supports_multiple_files = value

    def __str__(self):
        return ' '.join(f'{ii} {getattr(self, ii)}' for ii in vars(self))


class TaskType(Enum):
    """The possible steps in a Collection pipeline. A short-hand, user-facing
    way to identify the work to be done by a pipeline."""

    STORE = 'store'  # store a file to CADC
    SCRAPE = 'scrape'  # create/update a CAOM instance with no network
    INGEST = 'ingest'  # create/update a CAOM instance from metadata only
    MODIFY = 'modify'  # data access observation visitors
    VISIT = 'visit'  # metadata access observation visitors


class State:
    """Persist information between pipeline invocations.

    Currently the State class persists the concept of a bookmark, which is the
    place in the flow of data that was last processed. This 'place' may be a
    timestamp, or an id. That value is up to clients of this class.
    """

    def __init__(self, fqn, zone):
        self.fqn = fqn
        self._zone = zone
        self.bookmarks = {}
        self.logger = logging.getLogger(self.__class__.__name__)
        result = read_as_yaml(self.fqn)
        if result is None:
            raise CadcException(f'Could not load state from {fqn}')
        else:
            self.bookmarks = result.get('bookmarks')
            self.context = result.get('context', {})
            self.content = result

    def add_bookmark(self, key, value):
        if key not in self.bookmarks:
            self.bookmarks[key] = {}
        self.bookmarks[key]['last_record'] = value

    def get_bookmark(self, key):
        """Lookup for last_record key. Treat like an offset-aware datetime."""
        result = None
        if key in self.bookmarks:
            result = self.bookmarks.get(key).get('last_record')
            if result.tzinfo is not None:
                result = result.astimezone(self._zone).replace(tzinfo=None)
        else:
            self.logger.warning(f'No record found for {key}')
        return result

    def get_context(self, key):
        result = None
        if key in self.context:
            result = self.context.get(key)
        else:
            self.logger.warning(f'No context found for {key}')
        return result

    def save_state(self, key, value):
        """Write the current state as a YAML file.
        :param key which record is being updated
        :param value the value to update the record with
        """
        bookmarks = self.get_bookmark(key)
        if bookmarks is None:
            context = self.get_context(key)
            if context is None:
                self.logger.warning(f'No content found for {key}')
            else:
                self.context[key] = value
                self.logger.debug(f'Saving context {value} {self.fqn}')
                write_as_yaml(self.content, self.fqn)
        else:
            self.bookmarks[key]['last_record'] = value
            self.logger.debug(f'Saving bookmarked last record {value} {self.fqn}')
            write_as_yaml(self.content, self.fqn)

    def write_bookmarks(self, state_fqn):
        bookmark = { 'bookmarks': {}}
        for book_mark, last_record in self.bookmarks.items():
            bookmark['bookmarks'][book_mark] = {}
            bookmark['bookmarks'][book_mark]['last_record'] = last_record['last_record']
        write_as_yaml(bookmark, state_fqn)

    def write_content(self, state_fqn):
        write_as_yaml(self.content, state_fqn)

    @staticmethod
    def write_bookmark(state_fqn, book_mark, time_dt):
        bookmark = {
            'bookmarks': {
                f'{book_mark}': {'last_record': time_dt},
            }
        }
        write_as_yaml(bookmark, state_fqn)


class Rejected:
    """Persist information between pipeline invocations about the observation
    IDs that will fail a particular TaskType.
    """

    NO_REASON = ''
    BAD_DATA = 'bad_data'
    BAD_METADATA = 'bad_metadata'
    INVALID_FILE_NAME = 'invalid_file_name'
    INVALID_FORMAT = 'is_valid_fails'
    MISSING = 'missing_at_source'
    MYSTERY_VALUE = 'mystery_value'
    NO_INSTRUMENT = 'no_instrument'
    NO_PREVIEW = 'no_preview'
    OLD_VERSION = 'old_version'
    REJECTED = 'rejected'

    # A map to the logging message string representing acknowledged rejections
    reasons = {
        BAD_DATA: 'Header missing END card',
        BAD_METADATA: 'Cannot build an observation',
        INVALID_FILE_NAME: 'Invalid name format',
        INVALID_FORMAT: 'Invalid observation ID',
        MISSING: 'Could not find JSON record for',
        MYSTERY_VALUE: 'Unexpected value in enumerated type',
        NO_INSTRUMENT: 'Unknown value for instrument',
        NO_PREVIEW: 'Not Found for url: https://archive.gemini.edu/preview',
        OLD_VERSION: 'Recorded without checking',
        REJECTED: 'Rejected',
    }

    def __init__(self, fqn):
        """
        Ctor
        :param fqn: fully-qualified name for reading/writing rejected
        information records. If the file exists, initialize the in-memory
        records with the content of the file. Otherwise initialize the
        in-memory records to be empty.
        """
        self.fqn = fqn
        if os.path.exists(fqn):
            try:
                self.content = read_as_yaml(fqn)
            except yaml.constructor.ConstructorError:
                logging.error(f'ConstructorError reading {self.fqn}')
                self.content = {}
        else:
            dir_name = os.path.dirname(fqn)
            create_dir(dir_name)
            self.content = {}
        for reason in Rejected.reasons.keys():
            if reason not in self.content:
                self.content[reason] = []

    def __str__(self):
        return self.content.__str__()

    def check_and_record(self, message, obs_id):
        """Keep track of an additional entry.
        :returns boolean True if the failure is known and tracked."""
        for reason, reason_str in Rejected.reasons.items():
            if reason_str in message:
                self.record(reason, obs_id)
                return True
        return False

    def record(self, reason, entry):
        """Keep track of an additional entry."""
        self.content[reason].append(entry)

    def persist_state(self):
        """Write the current state as a YAML file."""
        for key, value in self.content.items():
            # ensure unique entries
            self.content[key] = list(set(value))
        write_as_yaml(self.content, self.fqn)

    def get_bad_data(self):
        return self.content[Rejected.BAD_DATA]

    def get_bad_metadata(self):
        return self.content[Rejected.BAD_METADATA]

    def get_no_preview(self):
        return self.content[Rejected.NO_PREVIEW]

    def is_bad_data(self, obs_id):
        return obs_id in self.content[Rejected.BAD_DATA]

    def is_bad_metadata(self, obs_id):
        return obs_id in self.content[Rejected.BAD_METADATA]

    def is_mystery_value(self, file_name):
        return file_name in self.content[Rejected.MYSTERY_VALUE]

    def is_no_preview(self, file_name):
        return file_name in self.content[Rejected.NO_PREVIEW]

    @staticmethod
    def known_failure(message):
        """Returns the REASON for the failure, or an empty string if
        the failure is of an unexpected type.
        """
        result = Rejected.NO_REASON
        for reason, text in Rejected.reasons.items():
            if text in message:
                result = reason
                break
        return result


class ExecutionReporter:
    """
    This class reports on the individual work unit successes, failures, and possible retries for the duration
    of a pipeline run.
    """

    def __init__(self, config, observable):
        """
        If a retry is configured, logging locations change during a pipeline run, so the file name setting is relegated
        to externally visible methods.
        """
        self._failure_fqn = None
        self._retry_fqn = None
        self._success_fqn = None
        self._report_fqn = config.report_fqn
        self._observable = observable
        self._summary = ExecutionSummary(os.path.basename(config.working_directory), config.collection)
        self.set_log_location(config)
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def all(self):
        return self._summary.entries

    @property
    def success(self):
        return self._summary.success

    def _count_retries(self):
        result = []
        if os.path.exists(self._retry_fqn):
            with open(self._retry_fqn) as f:
                result = f.readlines()
        return len(result)

    def _is_timeout(self, e):
        return e is not None and (
            'Read timed out' in e
            or 'reset by peer' in e
            or 'ConnectTimeoutError' in e
            or 'Broken pipe' in e
        )

    def _set_log_files(self, config):
        """Support changing log file locations during a retry."""
        self._success_fqn = config.success_fqn
        self._failure_fqn = config.failure_fqn
        self._retry_fqn = config.retry_fqn

    def reset_for_retry(self):
        self._summary.reset_for_retry()

    def set_log_location(self, config):
        """Support changing log file locations during a retry."""
        self._set_log_files(config)
        create_dir(config.log_file_directory)
        now_s = datetime.utcnow().timestamp()
        for fqn in [self._success_fqn, self._failure_fqn, self._retry_fqn, self._report_fqn]:
            ExecutionReporter._init_log_file(fqn, now_s)

    @staticmethod
    def _init_log_file(log_fqn, now_s):
        """Keep old versions of the progress files."""
        log_fid = log_fqn.replace('.txt', '')
        back_fqn = f'{log_fid}.{now_s}.txt'
        if os.path.exists(log_fqn) and os.path.getsize(log_fqn) != 0:
            move(log_fqn, back_fqn)
        f_handle = open(log_fqn, 'w')
        f_handle.close()

    def capture_failure(self, storage_name, e, stack_trace):
        """Log an error message to the failure file.

        If the failure is of a known type, also capture it to the rejected
        list. The rejected list will be saved to disk when the execute method
        completes.

        :obs_id observation ID being processed
        :file_name file name being processed
        :e Exception to log - the entire stack trace, which, if logging
            level is not set to debug, will be lost for debugging purposes.
        """
        self._logger.debug('Begin capture_failure')
        self._summary.add_errors(1)
        if self._is_timeout(stack_trace):
            self._summary.add_timeouts(1)
        with open(self._failure_fqn, 'a') as failure:
            if e.args is not None and len(e.args) > 1:
                min_error = e.args[0]
            else:
                min_error = str(e)
            failure.write(f'{datetime.now()} {storage_name.obs_id} {storage_name.file_name} {min_error}\n')

        # only retry entries that are not permanently marked as rejected
        reason = Rejected.known_failure(stack_trace)
        if reason == Rejected.NO_REASON or self._is_timeout(stack_trace):
            with open(self._retry_fqn, 'a') as retry:
                for entry in storage_name.source_names:
                    retry.write(f'{entry}\n')
        else:
            self._observable.rejected.record(reason, storage_name.obs_id)
            self._summary.add_rejections(1)
        self._logger.debug('End capture_failure')

    def capture_success(self, obs_id, file_name, start_time):
        """Capture, with a timestamp, the successful observations/file names that have been processed.
        :param obs_id str observation ID being processed
        :param file_name str file name being processed
        :param start_time int seconds since beginning of execution.
        """
        self._logger.debug('Begin capture_success')
        self._summary.add_successes(1)
        execution_s = datetime.utcnow().timestamp() - start_time
        success = open(self._success_fqn, 'a')
        try:
            success.write(f'{datetime.now()} {obs_id} {file_name} {execution_s:.2f}\n')
        finally:
            success.close()
        msg = (
            f'Progress - record {self._summary.success} of {self._summary.entries} records processed in '
            f'{execution_s:.2f} s.'
        )
        self._logger.debug('*' * len(msg))
        self._logger.info(msg)
        self._logger.debug('*' * len(msg))

    def capture_todo(self, todo, rejected, skipped):
        self._logger.debug(f'Begin capture_todo todo {todo}, rejected {rejected}, skipped {skipped}')
        self._summary.add_entries(todo + rejected + skipped)
        self._summary.add_rejections(rejected)
        self._summary.add_skipped(skipped)

    def report(self):
        msg = self._summary.report()
        self._logger.info(msg)
        write_to_file(self._report_fqn, msg)

    def capture_retry(self, retry_count):
        self._summary.add_retries(retry_count)

    def get_file_names_from_log_file(self, log_fqn):
        """
        :return: a list of file names from one of the failure, retry, or success log files.
        """
        logging.error('enter get_file_names_from_log_file')
        result = []
        with open(log_fqn) as f_in:
            for line in f_in:
                logging.error(line)
                bits = line.split()
                if log_fqn == self._retry_fqn:
                    result.append(bits[0])
                else:
                    if bits[3] == 'None':
                        result.append(bits[2])
                    else:
                        result.append(bits[3])
        return result


class ExecutionSummary:
    """
    This class generates summary reports of the successes and failures that occur during a pipeline run.
    """

    def __init__(self, location, collection):
        """
        - Execution time: the time from initiating the pipeline to completion of execution.
        - Number of inputs: the number of entries originally counted. Depending on the content of config.yml, this
             value will originate from a work file, or files on disk, or cumulative tracking of entries when working
             by state.
        - Number of successes: the number of entries that eventually succeed, including retries.
        - Number of timeouts: the number of times exceptions that may involve timeouts were generated. Those
             exceptions include messages like "Read timed out", "reset by peer", "ConnectTimeoutError", and
             "Broken pipe".
        - Number of retries: the number of retries executed. Depending on whether and how retry is enabled in
             config.yml, this value is the cumulative number of entries processed more than once.
        - Number of errors: the number of failures after the final retry. If this number is zero, a retry fixed the
             failures, so all entries were eventually ingested.
        - Number of rejections: the number of entries that are rejected due to well-known processing failures. These
             rejections include those caused by fitsverify or hd5check failures.
        - Number of skipped: the number of entries with a checksum that is the same at the data source as it is in
             CADC storage. If the checksum is the same, the pipeline can make no changes to either the data or metadata,
             so it doesn't try.
        """
        self._version = '0.0.0' if collection == 'TEST' else get_version(collection)
        self._location = location
        self._start_time = datetime.now(tz=tz.UTC).timestamp()
        self._entries_sum = 0
        self._errors_sum = 0
        self._rejected_sum = 0
        self._retry_sum = 0
        self._skipped_sum = 0
        self._success_sum = 0
        self._timeouts_sum = 0

    def __str__(self):
        return self.report()

    def add_entries(self, value):
        self._entries_sum += value

    def add_errors(self, value):
        self._errors_sum += value

    def add_rejections(self, value):
        self._rejected_sum += value

    def add_retries(self, value):
        self._retry_sum += value

    def add_skipped(self, value):
        self._skipped_sum += value

    def add_successes(self, value):
        self._success_sum += value

    def add_timeouts(self, value):
        self._timeouts_sum += value

    @property
    def success(self):
        return self._success_sum

    @property
    def entries(self):
        return self._entries_sum

    def report(self):
        msg1 = f'Location: {self._location}'
        msg2 = f'Date: {datetime.isoformat(datetime.utcnow())}'
        execution_time = datetime.now(tz=tz.UTC).timestamp() - self._start_time
        msg3 = f'Execution Time: {execution_time:.2f} s'
        msg4 = f'Version: {self._version}'
        msg5 = f'    Number of Inputs: {self._entries_sum}'
        msg6 = f' Number of Successes: {self._success_sum}'
        msg7 = f'  Number of Timeouts: {self._timeouts_sum}'
        msg8 = f'   Number of Retries: {self._retry_sum}'
        msg9 = f'    Number of Errors: {self._errors_sum}'
        msg10 = f'Number of Rejections: {self._rejected_sum}'
        msg11 = f'   Number of Skipped: {self._skipped_sum}'
        max_length = max(
            len(msg1),
            len(msg2),
            len(msg3),
            len(msg4),
            len(msg5),
            len(msg6),
            len(msg7),
            len(msg8),
            len(msg9),
            len(msg10),
            len(msg11),
        )
        msg_highlight = '*' * max_length
        msg = (
            f'\n\n{msg_highlight}\n{msg1}\n{msg2}\n{msg3}\n{msg4}\n{msg5}\n'
            f'{msg6}\n{msg7}\n{msg8}\n{msg9}\n{msg10}\n{msg11}\n{msg_highlight}\n\n'
        )
        return msg

    def reset_for_retry(self):
        # pass  TODO - not sure if this implementation is correct
        # don't know if this is the right thing to do
        self._success_sum = 0

    @staticmethod
    def read_report_file(fqn):
        summary = ExecutionSummary(os.path.basename(fqn), 'TEST')
        with open(fqn) as f_in:
            for line in f_in:
                if 'Number of' in line:
                    ct = int(line.split(':')[-1].strip())
                    if 'Number of Inputs' in line:
                        summary.add_entries(ct)
                    if 'Number of Successes' in line:
                        summary.add_successes(ct)
                    if 'Number of Timeouts' in line:
                        summary.add_timeouts(ct)
                    if 'Number of Retries' in line:
                        summary.add_retries(ct)
                    if 'Number of Errors' in line:
                        summary.add_errors(ct)
                    if 'Number of Rejections' in line:
                        summary.add_rejections(ct)
                    if 'Number of Skipped' in line:
                        summary.add_skipped(ct)
        return summary


class Metrics:
    """
    A class to capture execution metrics.
    """

    def __init__(self, config):
        self.enabled = config.observe_execution
        if self.enabled:
            self.history = {}
            self.failures = {}
            self.observable_dir = config.observable_directory

    def observe(self, start, stop, size, action, service, label):
        if self.enabled:
            elapsed = round(stop - start, 3)
            rate = round(size / (stop - start), 3)
            if service not in self.history:
                self.history[service] = {}
            if action not in self.history[service]:
                self.history[service][action] = {}
            self.history[service][action][label] = [elapsed, rate, start]

    def observe_failure(self, action, service, label):
        if self.enabled:
            if service not in self.failures:
                self.failures[service] = {}
            if action not in self.failures[service]:
                self.failures[service][action] = {}
            if label not in self.failures[service][action]:
                self.failures[service][action][label] = 1
            else:
                self.failures[service][action][label] += 1

    def capture(self):
        if self.enabled:
            create_dir(self.observable_dir)
            now = datetime.utcnow().timestamp()
            for service in self.history.keys():
                fqn = os.path.join(
                    self.observable_dir, f'{now}.{service}.yml'
                )
                write_as_yaml(self.history[service], fqn)

            fqn = os.path.join(self.observable_dir, f'{now}.fail.yml')
            write_as_yaml(self.failures, fqn)


def minimize_on_keyword(x, candidate):
    result = x
    if candidate is not None:
        if x is None:
            result = candidate
        else:
            result = min(x, candidate)
    return result


class Observable:
    """
    A class to contain all the classes that maintain information between pipeline execution instances, and
    any information to assist in debugging.
    """

    def __init__(self, config):
        self._rejected = Rejected(config.rejected_fqn)
        self._metrics = Metrics(config)
        self._meta_producer = get_version(config.collection)

    @property
    def rejected(self):
        return self._rejected

    @property
    def meta_producer(self):
        return self._meta_producer

    @property
    def metrics(self):
        return self._metrics


class Cache:
    """Abstract-like class to store persistent information that originates
    from outside of a pipeline invocation.
    """

    def __init__(self, rigorous_get=True):
        """ """
        config = Config()
        config.get_executors()
        self._fqn = config.cache_fqn
        # if True, raise exceptions, which tends to call a halt to any
        # pipeline. If False, only log a warning.
        self._rigorous_get = rigorous_get
        self._logger = logging.getLogger(self.__class__.__name__)
        try:
            self._cache = read_as_yaml(self._fqn)
        except Exception as e:
            raise CadcException(
                f'Cache file {self._fqn} read failure {e}. Stopping pipeline.'
            )

    def add_to(self, key, values):
        """Add to or update the content of the cache. This is an over-write
        operation."""
        self._cache[key] = values

    def get_from(self, key):
        result = self._cache.get(key)
        if result is None:
            msg = f'Failed to find key {key} in cache {self._fqn}.'
            if self._rigorous_get:
                raise CadcException(msg)
            else:
                self._logger.warning(msg)
                result = []
        return result

    def save(self):
        """Write the current state as a YAML file."""
        write_as_yaml(self._cache, self._fqn)


class CaomName:
    """The naming rules for making and decomposing CAOM URIs (i.e. Observation
    URIs, Plane URIs, and archive URIs, all isolated in one class. There are
    probably OMM assumptions built in, but those will slowly go away :)."""

    def __init__(self, uri):
        self.uri = uri
        ignore_scheme, ignore_path, file_name = decompose_uri(self.uri)
        self._file_name = file_name
        self._file_id = StorageName.remove_extensions(file_name)

    @property
    def file_id(self):
        """

        :return: Extracted from an Artifact URI, the file_id is the file
        name portion of the URI with all file type and compression type
        extensions removed.
        """
        return self._file_id

    @property
    def file_name(self):
        """:return The file name extracted from an Artifact URI."""
        return self._file_name

    @property
    def uncomp_file_name(self):
        """:return The file name extracted from an Artifact URI, without
        the compression extension."""
        return self._file_name.replace('.gz', '')

    @staticmethod
    def make_obs_uri_from_obs_id(collection, obs_id):
        """:return A string that conforms to the Observation URI
        specification from CAOM."""
        return f'caom:{collection}/{obs_id}'

    @staticmethod
    def decompose_provenance_input(uri):
        # looks like:
        # caom:OMM/C170323_0151_CAL/C170323_0151_CAL
        bits = uri.split('/')
        obs_id = bits[1]
        product_id = bits[2]
        return obs_id, product_id

    @staticmethod
    def extract_file_name(entry):
        """
        :param entry: may be a URL, a fully-qualified file name, or a simple
            file name. Extract the file name from which-ever one of those
            structures it is.
        :return:
        """
        temp = parse.urlparse(entry)
        if temp.scheme == '':
            file_name = os.path.basename(entry)
        else:
            if temp.scheme.startswith('http'):
                file_name = temp.path.split('/')[-1]
            else:
                # it's an Artifact URI
                file_name = temp.path.split('/')[-1]
        return file_name


class Config:
    """Configuration information that remains the same for all steps and all
    work in a pipeline execution."""

    def __init__(self):
        self._working_directory = None
        self._work_file = None
        # the fully qualified name for the work file
        self.work_fqn = None
        self._collection = None
        self._dump_blueprint = False
        self._use_local_files = False
        self._resource_id = None
        self._tap_id = None
        self._logging_level = None
        self._log_to_file = False
        self._log_file_directory = None
        self._storage_host = None
        self._task_types = []
        self._success_log_file_name = None
        # the fully qualified name for the file
        self.success_fqn = None
        self._failure_log_file_name = None
        # the fully qualified name for the file
        self.failure_fqn = None
        self._cleanup_files_when_storing = False
        self._report_fqn = None
        self._retry_file_name = None
        # the fully qualified name for the file
        self.retry_fqn = None
        self._retry_failures = False
        self._retry_count = 1
        self._retry_decay = 1
        self._proxy_file_name = None
        # the fully qualified name for the file
        self.proxy_fqn = None
        self._state_file_name = None
        # the fully qualified name for the file
        self.state_fqn = None
        self._rejected_file_name = None
        self._rejected_directory = None
        # the fully qualified name for the file
        self.rejected_fqn = None
        self.slack_channel = None
        self.slack_token = None
        self._store_modified_files_only = False
        self._progress_file_name = None
        self.progress_fqn = None
        self._interval = None
        self._observe_execution = False
        self._observable_directory = None
        self._source_host = None
        self._cache_file_name = None
        # the fully qualified name for the file
        self.cache_fqn = None
        self._data_read_groups = []
        self._meta_read_groups = []
        self._data_sources = []
        self._data_source_extensions = ['.fits']
        self._recurse_data_sources = True
        self._features = Features()
        self._cleanup_failure_destination = None
        self._cleanup_success_destination = None
        self._preview_scheme = 'cadc'
        self._scheme = 'cadc'
        self._storage_inventory_resource_id = None
        self._storage_inventory_tap_resource_id = None
        self._time_zone = tz.UTC
        self._total_retry_fqn = None

    @property
    def bookmark(self):
        return f'{self._collection.lower()}_timestamp'

    @property
    def is_connected(self):
        return TaskType.SCRAPE not in self._task_types

    @property
    def working_directory(self):
        """the root directory for all executor operations"""
        return self._working_directory

    @working_directory.setter
    def working_directory(self, value):
        self._working_directory = value

    @property
    def work_file(self):
        """the file that contains the list of work to be passed through the
        pipeline"""
        return self._work_file

    @work_file.setter
    def work_file(self, value):
        self._work_file = value
        if self._working_directory is not None:
            self.work_fqn = os.path.join(
                self._working_directory, self._work_file
            )

    @property
    def data_read_groups(self):
        return self._data_read_groups

    @data_read_groups.setter
    def data_read_groups(self, value):
        self._data_read_groups = value

    @property
    def meta_read_groups(self):
        return self._meta_read_groups

    @meta_read_groups.setter
    def meta_read_groups(self, value):
        self._meta_read_groups = value

    @property
    def data_sources(self):
        """Root URI for data retrieval"""
        return self._data_sources

    @data_sources.setter
    def data_sources(self, value):
        self._data_sources = value

    @property
    def data_source_extensions(self):
        """Root URI for data retrieval"""
        return self._data_source_extensions

    @data_source_extensions.setter
    def data_source_extensions(self, value):
        self._data_source_extensions = value

    @property
    def collection(self):
        """which collection is addressed by the pipeline"""
        return self._collection

    @collection.setter
    def collection(self, value):
        self._collection = value

    @property
    def dump_blueprint(self):
        return self._dump_blueprint

    @dump_blueprint.setter
    def dump_blueprint(self, value):
        self._dump_blueprint = value

    @property
    def use_local_files(self):
        """changes expectations of the executors for handling files on disk"""
        return self._use_local_files

    @use_local_files.setter
    def use_local_files(self, value):
        self._use_local_files = value

    @property
    def resource_id(self):
        """which service instance to use"""
        return self._resource_id

    @resource_id.setter
    def resource_id(self, value):
        self._resource_id = value

    @property
    def tap_id(self):
        """which tap service instance to use"""
        return self._tap_id

    @tap_id.setter
    def tap_id(self, value):
        self._tap_id = value

    @property
    def log_to_file(self):
        """boolean - write the log to a file?"""
        return self._log_to_file

    @log_to_file.setter
    def log_to_file(self, value):
        self._log_to_file = value

    @property
    def log_file_directory(self):
        """where log files are written to - defaults to working_directory"""
        return self._log_file_directory

    @log_file_directory.setter
    def log_file_directory(self, value):
        self._log_file_directory = value

    @property
    def logging_level(self):
        """the logging level - enforced throughout the pipeline"""
        return self._logging_level

    @logging_level.setter
    def logging_level(self, value):
        lookup = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
        }
        if value in lookup:
            self._logging_level = lookup[value]

    @property
    def cleanup_files_when_storing(self):
        """boolean - clean up files when finished with a possibly unsuccessful
        attempt to STORE."""
        return self._cleanup_files_when_storing

    @cleanup_files_when_storing.setter
    def cleanup_files_when_storing(self, value):
        self._cleanup_files_when_storing = value

    @property
    def cleanup_success_destination(self):
        """
        If cleanup_files_when_storing is set to True, after a successful
        STORE to CADC, the file will be 'mv'd to this destination. Probably
        a fully-qualified directory name, but HTTP is always a possibility.
        """
        return self._cleanup_success_destination

    @cleanup_success_destination.setter
    def cleanup_success_destination(self, value):
        self._cleanup_success_destination = value

    @property
    def cleanup_failure_destination(self):
        """
        If cleanup_files_when_storing is set to True, after a failed
        STORE to CADC, the file will be 'mv'd to this destination. Probably
        a fully-qualified directory name, but HTTP is always a possibility.
        """
        return self._cleanup_failure_destination

    @cleanup_failure_destination.setter
    def cleanup_failure_destination(self, value):
        self._cleanup_failure_destination = value

    @property
    def recurse_data_sources(self):
        """If true, will do a recursive search through `data_sources`."""
        return self._recurse_data_sources

    @recurse_data_sources.setter
    def recurse_data_sources(self, value):
        self._recurse_data_sources = value

    @property
    def storage_host(self):
        """the ad 'host' to store files to - used for testing cadc-data put
        commands only, should usually be None"""
        return self._storage_host

    @storage_host.setter
    def storage_host(self, value):
        self._storage_host = value

    @property
    def storage_inventory_resource_id(self):
        return self._storage_inventory_resource_id

    @storage_inventory_resource_id.setter
    def storage_inventory_resource_id(self, value):
        self._storage_inventory_resource_id = value

    @property
    def storage_inventory_tap_resource_id(self):
        return self._storage_inventory_tap_resource_id

    @storage_inventory_tap_resource_id.setter
    def storage_inventory_tap_resource_id(self, value):
        self._storage_inventory_tap_resource_id = value

    @property
    def task_types(self):
        """the way to control which steps get executed"""
        return self._task_types

    @task_types.setter
    def task_types(self, value):
        self._task_types = value

    @property
    def success_log_file_name(self):
        """the filename where success logs are written, this will be created
        in log_file_directory"""
        return self._success_log_file_name

    @success_log_file_name.setter
    def success_log_file_name(self, value):
        self._success_log_file_name = value
        if self._log_file_directory is not None:
            self.success_fqn = os.path.join(
                self._log_file_directory, self._success_log_file_name
            )

    @property
    def failure_log_file_name(self):
        """the filename where failure logs are written this will be created
        in log_file_directory"""
        return self._failure_log_file_name

    @failure_log_file_name.setter
    def failure_log_file_name(self, value):
        self._failure_log_file_name = value
        if self._log_file_directory is not None:
            self.failure_fqn = os.path.join(
                self._log_file_directory, self._failure_log_file_name
            )

    @property
    def report_fqn(self):
        return self._report_fqn

    @property
    def retry_file_name(self):
        """the filename where retry entries are written this will be created
        in log_file_directory"""
        return self._retry_file_name

    @retry_file_name.setter
    def retry_file_name(self, value):
        self._retry_file_name = value
        if self._log_file_directory is not None:
            self.retry_fqn = os.path.join(self._log_file_directory, self._retry_file_name)
            self._total_retry_fqn = os.path.join(self._log_file_directory, f'total_{value}')

    @property
    def retry_failures(self):
        """Will the application retry the entries in the
        retries.txt file? If True, the application will attempt to re-run
        the work to do for each entry in the retries.txt file. If False,
        it will do nothing."""
        return self._retry_failures

    @retry_failures.setter
    def retry_failures(self, value):
        self._retry_failures = value

    @property
    def retry_count(self):
        """how many times the application will retry the entries in the
        retries.txt file."""
        return self._retry_count

    @retry_count.setter
    def retry_count(self, value):
        self._retry_count = value

    @property
    def retry_decay(self):
        """factor applied to how long the application will wait before
        retrying the entries in the retries.txt file.

        Default is 1 minute, a value of 0.25 will result in a 15 second delay,
        a value of 10 will result in a 10 minute delay.
        """
        return self._retry_decay

    @retry_decay.setter
    def retry_decay(self, value):
        self._retry_decay = value

    @property
    def rejected_directory(self):
        """the directory where rejected entry files are written, by default
        will be the log file directory"""
        return self._rejected_directory

    @rejected_directory.setter
    def rejected_directory(self, value):
        self._rejected_directory = value
        if self._rejected_file_name is not None:
            if self._rejected_directory is not None:
                self.rejected_fqn = os.path.join(
                    self._rejected_directory, self._rejected_file_name
                )
            elif self._log_file_directory is not None:
                self.rejected_fqn = os.path.join(
                    self._log_file_directory, self._rejected_file_name
                )

    @property
    def rejected_file_name(self):
        """the filename where rejected entries are written, this will be created
        in log_file_directory"""
        return self._rejected_file_name

    @rejected_file_name.setter
    def rejected_file_name(self, value):
        self._rejected_file_name = value
        if self._rejected_directory is not None:
            self.rejected_fqn = os.path.join(
                self._rejected_directory, self._rejected_file_name
            )
        elif self._log_file_directory is not None:
            self.rejected_fqn = os.path.join(
                self._log_file_directory, self._rejected_file_name
            )

    @property
    def slack_channel(self):
        return self._slack_channel

    @slack_channel.setter
    def slack_channel(self, value):
        self._slack_channel = value

    @property
    def slack_token(self):
        return self._slack_token

    @slack_token.setter
    def slack_token(self, value):
        self._slack_token = value

    @property
    def store_modified_files_only(self):
        return self._store_modified_files_only

    @store_modified_files_only.setter
    def store_modified_files_only(self, value):
        self._store_modified_files_only = value

    @property
    def progress_file_name(self):
        """the filename where pipeline progress is written, this will be created
        in log_file_directory. Useful when using timestamp windows for
        execution."""
        return self._progress_file_name

    @progress_file_name.setter
    def progress_file_name(self, value):
        self._progress_file_name = value
        if self._log_file_directory is not None:
            self.progress_fqn = os.path.join(
                self._log_file_directory, self._progress_file_name
            )

    @property
    def proxy_file_name(self):
        """If using a proxy certificate for authentication, identify the
        fully-qualified pathname here."""
        return self._proxy_file_name

    @proxy_file_name.setter
    def proxy_file_name(self, value):
        self._proxy_file_name = value
        if (
            self._working_directory is not None
            and self._proxy_file_name is not None
        ):
            self.proxy_fqn = os.path.join(
                self._working_directory, self._proxy_file_name
            )

    @property
    def state_file_name(self):
        """If using a state file to communicate persistent information between
        invocations, identify the fully-qualified pathname here."""
        return self._state_file_name

    @state_file_name.setter
    def state_file_name(self, value):
        self._state_file_name = value
        if (
            self._working_directory is not None
            and self._state_file_name is not None
        ):
            self.state_fqn = os.path.join(
                self._working_directory, self._state_file_name
            )

    @property
    def cache_file_name(self):
        """If using a cache file to communicate persistent information between
        invocations, identify the fully-qualified pathname here."""
        return self._cache_file_name

    @cache_file_name.setter
    def cache_file_name(self, value):
        self._cache_file_name = value
        if (
            self._working_directory is not None
            and self._cache_file_name is not None
        ):
            self.cache_fqn = os.path.join(
                self._working_directory, self._cache_file_name
            )

    @property
    def features(self):
        """Feature flag setting access."""
        return self._features

    @features.setter
    def features(self, value):
        self._features = value

    @property
    def interval(self):
        """Interval - used for setting timestamp chunks, for now."""
        return self._interval

    @interval.setter
    def interval(self, value):
        self._interval = value

    @property
    def observe_execution(self):
        """If true, time and track the CADC service invocations."""
        return self._observe_execution

    @observe_execution.setter
    def observe_execution(self, value):
        self._observe_execution = value

    @property
    def observable_directory(self):
        """Directory where to track CADC service invocation information."""
        return self._observable_directory

    @observable_directory.setter
    def observable_directory(self, value):
        self._observable_directory = value

    @property
    def preview_scheme(self):
        """Preview scheme for Artifact URIs, which may be different based on who creates the file."""
        return self._preview_scheme

    @preview_scheme.setter
    def preview_scheme(self, value):
        self._preview_scheme = value

    @property
    def scheme(self):
        """Scheme for Artifact URIs"""
        return self._scheme

    @scheme.setter
    def scheme(self, value):
        self._scheme = value

    @property
    def source_host(self):
        """Host that is the source of something. Initial use case as ftp
        host name."""
        return self._source_host

    @source_host.setter
    def source_host(self, value):
        self._source_host = value

    @property
    def time_zone(self):
        """String that can be used in tz.gettz to get the timezone for a remote data source."""
        return self._time_zone

    @time_zone.setter
    def time_zone(self, value):
        self._time_zone = tz.gettz(value)

    @property
    def total_retry_fqn(self):
        return self._total_retry_fqn

    @property
    def use_vos(self):
        """"""
        result = False
        for entry in self._data_sources:
            if entry.startswith('vos:'):
                result = True
                break
        return result

    def __str__(self):
        return (
            f'\nFrom {os.getcwd()}/config.yml:\n'
            f'  cache_fqn:: {self.cache_fqn}\n'
            f'  cleanup_failure_destination:: '
            f'{self.cleanup_failure_destination}\n'
            f'  cleanup_files_when_storing:: '
            f'{self.cleanup_files_when_storing}\n'
            f'  cleanup_success_destination:: '
            f'{self.cleanup_success_destination}\n'
            f'  collection:: {self.collection}\n'
            f'  data_read_groups:: {self.data_read_groups}\n'
            f'  data_sources:: {self.data_sources}\n'
            f'  data_source_extensions:: {self.data_source_extensions}\n'
            f'  dump_blueprint:: {self.dump_blueprint}\n'
            f'  failure_fqn:: {self.failure_fqn}\n'
            f'  failure_log_file_name:: {self.failure_log_file_name}\n'
            f'  features:: {self.features}\n'
            f'  interval:: {self.interval}\n'
            f'  log_file_directory:: {self.log_file_directory}\n'
            f'  log_to_file:: {self.log_to_file}\n'
            f'  logging_level:: {self.logging_level}\n'
            f'  meta_read_groups:: {self.meta_read_groups}\n'
            f'  observable_directory:: {self.observable_directory}\n'
            f'  observe_execution:: {self.observe_execution}\n'
            f'  preview_scheme:: {self.preview_scheme}\n'
            f'  progress_file_name:: {self.progress_file_name}\n'
            f'  progress_fqn:: {self.progress_fqn}\n'
            f'  proxy_file_name:: {self.proxy_file_name}\n'
            f'  proxy_fqn:: {self.proxy_fqn}\n'
            f'  recurse_data_sources:: {self.recurse_data_sources}\n'
            f'  rejected_directory:: {self.rejected_directory}\n'
            f'  rejected_file_name:: {self.rejected_file_name}\n'
            f'  rejected_fqn:: {self.rejected_fqn}\n'
            f'  report_fqn:: {self.report_fqn}\n'
            f'  resource_id:: {self.resource_id}\n'
            f'  retry_count:: {self.retry_count}\n'
            f'  retry_decay:: {self.retry_decay}\n'
            f'  retry_failures:: {self.retry_failures}\n'
            f'  retry_file_name:: {self.retry_file_name}\n'
            f'  retry_fqn:: {self.retry_fqn}\n'
            f'  scheme:: {self.scheme}\n'
            f'  slack_channel:: {self.slack_channel}\n'
            f'  slack_token:: secret\n'
            f'  source_host:: {self.source_host}\n'
            f'  state_file_name:: {self.state_file_name}\n'
            f'  state_fqn:: {self.state_fqn}\n'
            f'  storage_inventory_resource_id:: {self.storage_inventory_resource_id}\n'
            f'  storage_inventory_tap_resource_id:: {self.storage_inventory_tap_resource_id}\n'
            f'  store_modified_files_only:: {self.store_modified_files_only}\n'
            f'  success_fqn:: {self.success_fqn}\n'
            f'  success_log_file_name:: {self.success_log_file_name}\n'
            f'  tap_id:: {self.tap_id}\n'
            f'  task_types:: {self.task_types}\n'
            f'  time_zone:: {self.time_zone}\n'
            f'  total_retry_fqn:: {self.total_retry_fqn}\n'
            f'  use_local_files:: {self.use_local_files}\n'
            f'  work_fqn:: {self.work_fqn}\n'
            f'  working_directory:: {self.working_directory}'
        )

    @staticmethod
    def _obtain_list(key, config, default=[]):
        """Make the configuration file entries into the Enum."""
        result = []
        if key in config:
            for ii in config[key]:
                result.append(ii)
        else:
            result = default
        return list(set(result))

    @staticmethod
    def _obtain_task_types(config, default=None):
        """Make the configuration file entries into the Enum."""
        task_types = []
        if 'task_types' in config:
            for ii in config['task_types']:
                task_types.append(TaskType(ii))
            return task_types
        else:
            return default

    @staticmethod
    def _obtain_features(config):
        """Make the configuration file entries into the class members."""
        feature_flags = Features()
        if 'features' in config:
            for ii in config['features']:
                feature = f'_{ii}'
                if hasattr(feature_flags, feature):
                    setattr(feature_flags, feature, config['features'][ii])
                else:
                    logging.warning(f'Unexpected features item:{ii}.')
        return feature_flags

    def change_working_directory(self, new_directory):
        """Mostly for test support."""
        def _set_value(existing, new_name):
            return existing if existing is not None else new_name

        self.working_directory = new_directory
        self.work_file = _set_value(self._work_file, 'todo.txt')
        self.state_file_name = _set_value(self._state_file_name, 'state.yml')

        self._log_file_directory = f'{new_directory}/logs'
        self.failure_log_file_name = _set_value(self._failure_log_file_name, 'failure_log.txt')
        self.progress_file_name = _set_value(self._progress_file_name, 'progress.txt')
        self.retry_file_name = _set_value(self._retry_file_name, 'retries.txt')
        self.success_log_file_name = _set_value(self._success_log_file_name, 'success_log.txt')

        self._report_fqn = os.path.join(
            self.log_file_directory, f'{os.path.basename(self.working_directory)}_report.txt'
        )

        self.rejected_directory = f'{new_directory}/rejected'
        self.rejected_file_name = _set_value(self._rejected_file_name, 'rejected.yml')
        self.observable_directory = f'{new_directory}/metrics'

    def get(self):
        """Look up the configuration values in the data structure extracted
        from the configuration file."""
        return self.get_executors()

    def get_executors(self):
        """Look up the configuration values in the data structure extracted
        from the configuration file.

        Consider this deprecated - use get instead, because the name is
        non-representative of the work being done.
        """
        try:
            config = self.get_config()
            self.working_directory = config.get(
                'working_directory', os.getcwd()
            )
            self.work_file = config.get('todo_file_name', 'todo.txt')
            self.data_sources = Config._obtain_list(
                'data_sources', config, []
            )
            self.data_source_extensions = Config._obtain_list(
                'data_source_extensions', config, ['.fits']
            )
            self.resource_id = config.get(
                'resource_id', 'ivo://cadc.nrc.ca/sc2repo'
            )
            self.tap_id = config.get('tap_id', 'ivo://cadc.nrc.ca/sc2tap')
            self.use_local_files = bool(config.get('use_local_files', False))
            self.cleanup_failure_destination = config.get(
                'cleanup_failure_destination', None
            )
            self.cleanup_files_when_storing = bool(
                config.get('cleanup_files_when_storing', False)
            )
            self.cleanup_success_destination = config.get(
                'cleanup_success_destination', None
            )
            self.data_read_groups = config.get('data_read_groups', [])
            self.dump_blueprint = config.get('dump_blueprint', False)
            self.logging_level = config.get('logging_level', 'DEBUG')
            self.log_to_file = config.get('log_to_file', False)
            self.log_file_directory = config.get(
                'log_file_directory', self.working_directory
            )
            self.meta_read_groups = config.get('meta_read_groups', [])
            self.task_types = Config._obtain_task_types(config, [])
            self.collection = config.get('collection', 'TEST')
            self.success_log_file_name = config.get(
                'success_log_file_name', 'success_log.txt'
            )
            self.failure_log_file_name = config.get(
                'failure_log_file_name', 'failure_log.txt'
            )
            self.retry_file_name = config.get(
                'retry_file_name', 'retries.txt'
            )
            self.retry_failures = config.get('retry_failures', False)
            self.retry_count = config.get('retry_count', 1)
            self.retry_decay = config.get('retry_decay', 1)
            self.rejected_file_name = config.get(
                'rejected_file_name', 'rejected.yml'
            )
            self.rejected_directory = config.get(
                'rejected_directory', os.getcwd()
            )
            self.progress_file_name = config.get(
                'progress_file_name', 'progress.txt'
            )
            self.interval = config.get('interval', 10)
            self.features = self._obtain_features(config)
            self.proxy_file_name = config.get('proxy_file_name', None)
            self.state_file_name = config.get('state_file_name', None)
            self.cache_file_name = config.get('cache_file_name', None)
            self.observe_execution = config.get('observe_execution', False)
            self.observable_directory = config.get(
                'observable_directory', None
            )
            self.recurse_data_sources = config.get(
                'recurse_data_sources', False
            )
            self.slack_channel = config.get('slack_channel', None)
            self.slack_token = config.get('slack_token', None)
            self.source_host = config.get('source_host', None)
            self.storage_inventory_resource_id = config.get(
                'storage_inventory_resource_id',
                'ivo://cadc.nrc.ca/global/raven',
            )
            self.storage_inventory_tap_resource_id = config.get(
                'storage_inventory_tap_resource_id', 'ivo://cadc.nrc.ca/global/luskan'
            )
            self.store_modified_files_only = config.get(
                'store_modified_files_only', False
            )
            self.preview_scheme = config.get('preview_scheme', 'cadc')
            self._report_fqn = os.path.join(
                self.log_file_directory,
                f'{os.path.basename(self.working_directory)}_report.txt',
            )
            self.scheme = config.get('scheme', 'cadc')
            self.time_zone = config.get('time_zone', 'UTC')
        except KeyError as e:
            raise CadcException(f'Error in config file {e}')

        logger = logging.getLogger()
        logger.setLevel(self.logging_level)
        logging.debug(self)

    def get_config(self):
        """Return a configuration dictionary. Assumes a file named config.yml
        in the current working directory."""
        config_fqn = os.path.join(os.getcwd(), 'config.yml')
        config = self.load_config(config_fqn)
        if config is None:
            raise CadcException(f'Could not find the file {config_fqn}')
        return config

    def need_to_retry(self):
        """Evaluate the need to have the pipeline try to re-execute for any
         files/observations that have been logged as failures.

        If log_to_file is not set to True, there is no retry file content
        to retry on.

         :return True if the configuration and logging information indicate a
            need to attempt to retry the pipeline execution for any entries.
        """
        result = True
        if self.retry_failures:
            meta = get_file_meta(self.retry_fqn)
            if meta['size'] == 0:
                logging.info(
                    f'Checked the retry file {self.retry_fqn}. There are no '
                    f'logged failures.'
                )
                result = False
        else:
            result = False
        return result

    def update_for_retry(self, count):
        """
        When retrying, the application will:

        - use the retries.txt file as the todo list
        - retry as many times as the 'retry count' in the config.yml file.
        - make a new log directory, in the working directory, with the name
            logs_{retry_count}. Any failures for the retry execution that
            need to be logged will be logged here.
        - in the new log directory, make a new .xml file for the
            output, with the name {obs_id}.xml

        :param count the current retry iteration
        """
        self.work_file = self.retry_file_name
        self.work_fqn = self.retry_fqn
        if '_' in os.path.basename(self.log_file_directory):
            temp = self.log_file_directory.rsplit('_', 1)[0]
            self.log_file_directory = f'{temp}_{count}'
        else:
            self.log_file_directory = f'{self.log_file_directory}_{count}'
        # reset the location of the log file names
        self.success_log_file_name = self.success_log_file_name
        self.failure_log_file_name = self.failure_log_file_name
        self.retry_file_name = self.retry_file_name

        logging.info(f'Retry work file is {self.work_fqn}')
        logging.debug(
            f'Retry logging files are:\n  '
            f'success: {self.success_fqn}\n  '
            f'failure: {self.failure_fqn}\n  '
            f'retry:   {self.retry_fqn}'
        )

    @staticmethod
    def load_config(config_fqn):
        """Read a configuration as a YAML file.
        :param config_fqn the fully qualified name for the configuration
            file.
        """
        try:
            logging.debug(f'Begin load_config from {config_fqn}.')
            with open(config_fqn) as f:
                data_map = yaml.safe_load(f)
                logging.debug('End load_config.')
                return data_map
        except (yaml.scanner.ScannerError, FileNotFoundError) as e:
            logging.error(e)
            return None

    @staticmethod
    def write_to_file(config):
        """Avoid specifying types when writing a config.yml file."""
        config_fqn = os.path.join(os.getcwd(), 'config.yml')
        with open(config_fqn, 'w') as f:
            for entry in dir(config):
                try:
                    attribute = getattr(config, entry)
                except TypeError:
                    pass
                if entry.startswith('_') or callable(attribute):
                    continue
                elif entry == 'features':
                    if len(dir(attribute)) == 0:
                        f.write('features: []\n')
                    else:
                        f.write('features:\n')
                        for feature in dir(attribute):
                            try:
                                feature_attribute = getattr(attribute, feature)
                            except TypeError:
                                pass
                            if feature.startswith('_') or callable(
                                feature_attribute
                            ):
                                continue
                            f.write(f'  {feature}: {feature_attribute}\n')
                elif entry == 'task_types':
                    if len(attribute) > 0:
                        f.write('task_types:\n')
                        for task in attribute:
                            f.write(f'  - {task.name.lower()}\n')
                elif entry == 'logging_level':
                    lookup = {
                        logging.DEBUG: 'DEBUG',
                        logging.INFO: 'INFO',
                        logging.WARNING: 'WARNING',
                        logging.ERROR: 'ERROR',
                    }
                    temp = lookup.get(attribute)
                    f.write(f'{entry}: {temp}\n')
                elif attribute is not None:
                    f.write(f'{entry}: {attribute}\n')


@dataclass
class PreviewMeta:
    f_name: str
    product_type: ProductType
    release_type: ReleaseType
    mime_type: str = 'image/jpeg'


class PreviewVisitor:
    """
    Common code for creating thumbnails and previews. Must be extended with
    the code that does the actual generation of thumbnail and preview
    images.

    This code takes care of adding artifacts to the Observation, placing the
    files in CADC storage, and cleaning up things left behind on disk.
    """

    def __init__(self, release_type=None, mime_type='image/jpeg', **kwargs):
        self._release_type = release_type
        self._mime_type = mime_type
        self._logger = logging.getLogger(self.__class__.__name__)
        self._working_dir = kwargs.get('working_directory', './')
        self._clients = kwargs.get('clients')
        if self._clients is None or self._clients.data_client is None:
            self._logger.warning(
                'Visitor needs a clients.data_client parameter to store previews.'
            )
        self._storage_name = kwargs.get('storage_name')
        if self._storage_name is None:
            raise CadcException('Visitor needs a storage_name parameter.')
        self._metadata_reader = kwargs.get('metadata_reader')
        self._science_file = self._storage_name.file_name
        self._science_fqn = self._storage_name.get_file_fqn(self._working_dir)
        self._preview_fqn = os.path.join(
            self._working_dir, self._storage_name.prev
        )
        self._thumb_fqn = os.path.join(
            self._working_dir, self._storage_name.thumb
        )
        self._delete_list = []
        # keys are uris, values are lists, where the 0th entry is a file name,
        # and the 1th entry is the artifact type
        self._previews = {}
        self._report = None
        self._hdu_list = None
        self._ext = None
        self._logger.debug(self)

    @property
    def report(self):
        return self._report

    def __str__(self):
        return (
            f'\nworking directory: {self._working_dir}\n'
            f'science file: {self._storage_name.file_name}'
        )

    def visit(self, observation):
        check_param(observation, Observation)
        count = 0
        if self._storage_name.product_id in observation.planes.keys():
            plane = observation.planes[self._storage_name.product_id]
            if self._storage_name.file_uri in plane.artifacts.keys():
                self._logger.debug(
                    f'Preview generation for observation '
                    f'{observation.observation_id}, plane {plane.product_id}.'
                )
                count += self._do_prev(plane, observation.observation_id)
            self._augment_artifacts(plane)
            self._delete_list_of_files()
        self._logger.info(
            f'Completed preview augmentation for {observation.observation_id}. Changed {count} artifacts.'
        )
        self._report = {'artifacts': count}
        return observation

    def add_preview(
        self,
        uri,
        f_name,
        product_type,
        release_type=None,
        mime_type='image/jpeg',
    ):
        preview_meta = PreviewMeta(
            f_name, product_type, release_type, mime_type
        )
        self._previews[uri] = preview_meta

    def add_to_delete(self, fqn):
        self._delete_list.append(fqn)

    def _augment_artifacts(self, plane):
        """Add/update the artifact metadata in the plane."""
        for uri, entry in self._previews.items():
            temp = None
            if uri in plane.artifacts:
                temp = plane.artifacts[uri]
            f_name = entry.f_name
            product_type = entry.product_type
            release_type = self._release_type
            if self._release_type is None:
                release_type = entry.release_type
            fqn = os.path.join(self._working_dir, f_name)
            plane.artifacts[uri] = get_artifact_metadata(
                fqn, product_type, release_type, uri, temp
            )

    def _delete_list_of_files(self):
        """Clean up files on disk after."""
        # cadc_client will be None if executing a ScrapeModify task, so
        # leave the files behind so the user can see them on disk.
        if (
            self._clients is not None
            and self._clients.data_client is not None
        ):
            for entry in self._delete_list:
                if os.path.exists(entry):
                    self._logger.warning(f'Deleting {entry}')
                    os.unlink(entry)

    def _do_prev(self, plane, obs_id):
        self.generate_plots(obs_id)
        if self._hdu_list is not None:
            # astropy says https://docs.astropy.org/en/stable/io/fits/index.html#working-with-large-files
            self._hdu_list.close()
            del self._hdu_list[self._ext].data
            del self._hdu_list
        self._store_smalls()
        return len(self._previews)

    def generate_plots(self, obs_id):
        raise NotImplementedError

    @property
    def storage_name(self):
        return self._storage_name

    def _store_smalls(self):
        if (
            self._clients is not None
            and self._clients.data_client is not None
        ):
            for uri, entry in self._previews.items():
                self._clients.data_client.put(self._working_dir, uri)

    def _gen_thumbnail(self):
        self._logger.debug(
            f'Generating thumbnail for file {self._science_fqn}.'
        )
        count = 0
        if os.path.exists(self._preview_fqn):
            # keep import local
            import matplotlib.image as image

            thumb = image.thumbnail(
                self._preview_fqn, self._thumb_fqn, scale=0.25
            )
            if thumb is not None:
                count = 1
        else:
            self._logger.warning(
                f'Could not find {self._preview_fqn} for thumbnail '
                f'generation.'
            )
        return count

    def _save_figure(self):
        self.add_to_delete(self._preview_fqn)
        count = 1
        self.add_preview(
            self._storage_name.prev_uri,
            self._storage_name.prev,
            ProductType.PREVIEW,
            ReleaseType.DATA,
        )
        count += self._gen_thumbnail()
        if count == 2:
            self.add_preview(
                self._storage_name.thumb_uri,
                self._storage_name.thumb,
                ProductType.THUMBNAIL,
                ReleaseType.META,
            )
            self.add_to_delete(self._thumb_fqn)
        return count


class StorageName:
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

    def __init__(
        self,
        obs_id=None,
        product_id=None,
        file_name=None,
        source_names=[],
    ):
        """
        :param obs_id: string value for Observation.observationID
        :param product_id: string value for Plane.productID
        :param file_name: string value of file name
        :param source_names: list of str - the fully-qualified representation
            of files, as represented at the source. Sufficient for retrieval,
            probably includes a scheme.
        """
        self._obs_id = obs_id
        self._product_id = product_id
        self._file_name = file_name
        self._source_names = source_names
        # list of str - the Artifact URIs as represented at CADC. Sufficient
        # for storing/retrieving to/from CADC.
        self._destination_uris = []
        # str - the file name with all file type and compression extensions
        # removed
        self._file_id = None
        self._logger = logging.getLogger(self.__class__.__name__)
        self.set_destination_uris()
        self.set_file_id()
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

    def _get_uri(self, file_name, scheme):
        return build_uri(scheme=scheme, archive=StorageName.collection, file_name=file_name)

    @property
    def file_id(self):
        """The file name with all file type and compression extensions removed"""
        return self._file_id

    @property
    def file_uri(self):
        """The CADC Storage URI for the file."""
        return self._get_uri(
            self._file_name.replace('.gz', '').replace('.bz2', '').replace('.header', ''), StorageName.scheme
        )

    @property
    def file_name(self):
        """The file name."""
        return self._file_name

    @property
    def destination_uris(self):
        return self._destination_uris

    @property
    def hdf5(self):
        return StorageName.is_hdf5(self._file_name)

    @property
    def model_file_name(self):
        """The file name used on local disk that holds the CAOM2 Observation
        XML."""
        return f'{self._obs_id}.xml'

    @property
    def prev(self):
        """The preview file name for the file."""
        return f'{self._obs_id}_prev.jpg'

    @property
    def thumb(self):
        """The thumbnail file name for the file."""
        return f'{self._obs_id}_prev_256.jpg'

    @property
    def prev_uri(self):
        """The preview URI."""
        return self._get_uri(self.prev, StorageName.preview_scheme)

    @property
    def thumb_uri(self):
        """The thumbnail URI."""
        return self._get_uri(self.thumb, StorageName.preview_scheme)

    @property
    def obs_id(self):
        """The observation ID associated with the file name."""
        return self._obs_id

    @obs_id.setter
    def obs_id(self, value):
        self._obs_id = value

    @property
    def log_file(self):
        """The log file name used when running any of the 'execute' steps."""
        return f'{self._obs_id}.log'

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
        self.set_destination_uris()

    @property
    def is_feasible(self):
        """
        To support the exclusion of CFHT HDF5 files in the pipeline.
        :return:
        """
        return True

    def is_valid(self):
        """:return True if the observation ID conforms to naming rules."""
        pattern = re.compile(StorageName.collection_pattern)
        return pattern.match(self._file_name)

    def get_file_fqn(self, working_directory):
        # if the decompressed file exists, use that
        # else, if the compressed file exists, use that

        # file_uri is the file name without the compression extension
        temp_f_name = os.path.basename(self.file_uri)
        temp_fqn = os.path.join(working_directory, temp_f_name)
        temp_obs_fqn = os.path.join(os.path.join(working_directory, self._obs_id), temp_f_name)
        fqn = temp_fqn
        if (
            self._source_names is not None
            and len(self._source_names) > 0
            and os.path.exists(self._source_names[0])
            # is there an interim, uncompressed file name?
            and self._source_names[0].endswith(temp_f_name)
        ):
            fqn = self._source_names[0]
        elif os.path.exists(temp_fqn):
            fqn = temp_fqn
        elif os.path.exists(temp_obs_fqn):
            fqn = temp_obs_fqn
        elif os.path.exists(self._source_names[0]):
            # use the compressed file, if it can be found
            fqn = self._source_names[0]
        return fqn

    def set_destination_uris(self):
        for entry in self._source_names:
            temp = parse.urlparse(entry)
            if '.fits' in entry:
                self._destination_uris.append(
                    self._get_uri(
                        os.path.basename(temp.path).replace('.gz', '').replace('.bz2', '').replace('.header', ''),
                        StorageName.scheme,
                    )
                )
            else:
                self._destination_uris.append(self._get_uri(os.path.basename(temp.path), StorageName.scheme))

    def set_file_id(self):
        if self._file_id is None:
            if self._file_name is not None:
                self._file_id = StorageName.remove_extensions(self._file_name)
            elif self._obs_id is not None:
                self._file_id = self._obs_id

    def set_obs_id(self, **kwargs):
        if self._obs_id is None:
            self._obs_id = self._file_id

    def set_product_id(self, **kwargs):
        if self._product_id is None:
            self._product_id = self._file_id

    @staticmethod
    def remove_extensions(name):
        """How to get the file_id from a file_name."""
        return (
            name.replace('.fits', '')
            .replace('.gz', '')
            .replace('.header', '')
        )

    @staticmethod
    def is_hdf5(entry):
        return '.hdf5' in entry or '.h5' in entry

    @staticmethod
    def is_preview(entry):
        return '.jpg' in entry


def compare_observations(actual_fqn, expected_fqn):
    """Compare the observation captured in actual_fqn with the observation
    captured in the expected_fqn. Returns the differences as a pretty
    string for logging.
    """
    actual = read_obs_from_file(actual_fqn)
    expected = read_obs_from_file(expected_fqn)
    result = get_differences(expected, actual, 'Observation')
    msg = None
    if result:
        compare_text = '\n'.join([r for r in result])
        msg = (
            f'Differences found in observation {expected.observation_id}\n'
            f'{compare_text}'
        )
    return msg


def to_float(value):
    """Cast to float, without throwing an exception."""
    result = None
    if value is not None:
        if isinstance(value, float):  #  or isinstance(value, int):
            result = value
        elif (isinstance(value, str) and len(value.strip()) > 0) or (
            isinstance(value, int)
        ):
            result = float(value)
    return result


def to_int(value):
    """Cast to int, without throwing an exception."""
    return int(value) if value is not None else None


def to_str(value):
    """Cast to str, without throwing an exception."""
    return str(value) if value is not None else None


def extract_file_name_from_uri(uri_str):
    return parse.urlparse(uri_str).path.split('/')[-1]


def exec_cmd(cmd, log_level_as=logging.debug, timeout=None):
    """
    This does command execution as a subprocess call.

    :param cmd the text version of the command being executed
    :param log_level_as control the logging level from the exec call
    :param timeout value in seconds, after which the process is terminated
        raising the TimeoutExpired exception.
    :return None
    """
    logging.debug(cmd)
    cmd_array = cmd.split()
    exec_cmd_array(cmd_array, log_level_as, timeout)


def exec_cmd_array(cmd_array, log_level_as=logging.debug, timeout=None):
    """
    This does command execution as a subprocess call.

    :param cmd_array array of the command being executed, usually because
        it's of the form ['/bin/bash', '-c', 'what you really want to do']
    :param log_level_as control the logging level from the exec call
    :param timeout value in seconds, after which the process is terminated
        raising the TimeoutExpired exception.
    :return None
    """
    cmd_text = ' '.join(ii for ii in cmd_array)
    try:
        child = subprocess.Popen(
            cmd_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        try:
            output, outerr = child.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            logging.warning(f'Command {cmd_array} timed out.')
            # child process is not killed if the timeout expires, so kill the
            # process and finish communication
            child.kill()
            child.stdout.close()
            child.stderr.close()
            output, outerr = child.communicate()
        if len(output) > 0:
            log_level_as(f'stdout {output.decode("utf-8")}')
        if len(outerr) > 0:
            logging.error(f'stderr {outerr.decode("utf-8")}')
        if child.returncode != 0 and child.returncode != -9:
            # not killed due to a timeout
            logging.warning(
                f'Command {cmd_text} failed with {child.returncode}.'
            )
            raise CadcException(
                f'Command {cmd_text} ::\nreturncode {child.returncode}, '
                f'\nstdout {output.decode("utf-8")}\' \nstderr '
                f'{outerr.decode("utf-8")}\''
            )
    except Exception as e:
        if isinstance(e, CadcException):
            raise e
        logging.warning(f'Error with command {cmd_text}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not execute cmd {cmd_text}. Exception {e}'
        )


def exec_cmd_info(cmd):
    """
    This does command execution as a subprocess call.

    :param cmd the text version of the command being executed
    :return The text from stdout.
    """
    logging.debug(cmd)
    cmd_array = cmd.split()
    try:
        output, outerr = subprocess.Popen(
            cmd_array, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        ).communicate()
        if outerr is not None and len(outerr) > 0 and outerr[0] is not None:
            raise CadcException(
                f'Command {cmd} had stderr {outerr.decode("utf-8")}'
            )
        if output is not None and len(output) > 0:
            return output.decode('utf-8')
    except Exception as e:
        logging.debug(f'Error with command {cmd}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(f'Could not execute cmd {cmd}. Exception {e}')


def exec_cmd_redirect(cmd, fqn):
    """
    This does command execution as a subprocess call. It redirects stdout
    to fqn, and assumes binary output for the re-direct.

    :param cmd the text version of the command being executed
    :param fqn the fully-qualified name of the file to which stdout is
        re-directed
    :return None
    """
    logging.debug(cmd)
    cmd_array = cmd.split()
    try:
        with open(fqn, 'wb') as outfile:
            outerr = subprocess.Popen(
                cmd_array, stdout=outfile, stderr=subprocess.PIPE
            ).communicate()
            if (
                outerr is not None
                and len(outerr) > 0
                and outerr[0] is not None
            ):
                logging.debug(
                    f'Command {cmd} had stderr {outerr.decode("utf-8")}'
                )
                raise CadcException(
                    f'Command {cmd} had outerr {outerr.decode("utf-8")}'
                )
    except Exception as e:
        logging.debug(f'Error with command {cmd}:: {e}')
        logging.debug(traceback.format_exc())
        raise CadcException(f'Could not execute cmd {cmd}.Exception {e}')


def ftp_get(ftp_host_name, source_fqn, dest_fqn):
    """
    :param ftp_host_name name from which to originate the FTP transfer. Assume
        anonymous login.
    :param source_fqn fully-qualified name on the FTP host, of the file to be
        transferred.
    :param dest_fqn fully-qualified name, locally valid, for where to transfer
        the file to.

    Uses ftputil, which always transfers files in binary mode.
    """
    # remove the need to have ftputil libraries on EVERY *2caom2 pipeline
    from ftputil import FTPHost

    try:
        with FTPHost(ftp_host_name, 'anonymous', '@anonymous') as ftp_host:
            ftp_host.download(source_fqn, dest_fqn)
            source_stats = ftp_host.stat(source_fqn)
            ftp_host.close()
            dest_meta = get_file_meta(dest_fqn)
            if source_stats.st_size == dest_meta.get('size'):
                logging.info(f'Downloaded {source_fqn} from {ftp_host_name}')
            else:
                os.unlink(dest_fqn)
                raise CadcException(
                    f'File size error when transferring {source_fqn} from '
                    f'{ftp_host_name}'
                )
    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not transfer {source_fqn} from {ftp_host_name}'
        )


def ftp_get_timeout(ftp_host_name, source_fqn, dest_fqn, timeout=20):
    """
    :param ftp_host_name name from which to originate the FTP transfer. Assume
        anonymous login.
    :param source_fqn fully-qualified name on the FTP host, of the file to be
        transferred.
    :param dest_fqn fully-qualified name, locally valid, for where to transfer
        the file to.
    :param timeout in seconds for blocking operations

    Uses ftplib, which supports specifying timeouts in the connection.
    """
    # remove the need to have ftputil libraries on EVERY *2caom2 pipeline
    from ftplib import FTP

    try:
        with FTP(ftp_host_name, timeout=timeout) as ftp_host:
            ftp_host.login()
            with open(dest_fqn, 'wb') as fp:
                ftp_host.retrbinary(f'RETR {source_fqn}', fp.write)
            ftp_host.voidcmd('TYPE I')
            source_size = ftp_host.size(source_fqn)
            ftp_host.quit()
            dest_meta = get_file_meta(dest_fqn)
            if source_size == dest_meta.get('size'):
                logging.info(f'Downloaded {source_fqn} from {ftp_host_name}')
            else:
                os.unlink(dest_fqn)
                raise CadcException(
                    f'File size error when transferring {source_fqn} from '
                    f'{ftp_host_name}'
                )
    except Exception as e:
        logging.error(e)
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not transfer {source_fqn} from {ftp_host_name}'
        )


def get_cadc_headers(uri):
    """
    Creates the FITS headers object by fetching the FITS headers of a CADC
    file. The function takes advantage of the fhead feature of the CADC
    storage service and retrieves just the headers and no data, minimizing
    the transfer time.

    The file must be public, because the header retrieval is done as an
    anonymous user.

    :param uri: CADC URI
    :return: a string of keyword/value pairs.
    """
    file_url = parse.urlparse(uri)
    # create possible types of subjects
    subject = net.Subject()
    client = CadcDataClient(subject)
    # do a fhead on the file
    archive, file_id = file_url.path.split('/')
    return get_cadc_headers_client(archive, file_id, client)


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
    Gets contentType, contentLength and contentChecksum of a CADC artifact
    :param client: CadcDataClient instance
    :param archive: archive file has been stored to
    :param fname: name of file in the archive
    :return:
    """
    return client.get_file_info(archive, fname)


def get_file_meta(fqn):
    """
    Gets contentType, contentLength and contentChecksum of an artifact on disk.

    At the time of writing, libmagic gets confused with gzip'd FITS files,
    and identifies them as application/octet-stream, so, just have a bunch
    of hard-coding instead.

    :param fqn: Fully-qualified name of the file for which to get the metadata.
    :return:
    """
    if fqn is None or not os.path.exists(fqn):
        raise CadcException(f'Could not find {fqn} in get_file_meta')
    meta = {
        'size': get_file_size(fqn),
        'md5sum': md5(open(fqn, 'rb').read()).hexdigest(),
    }
    if (
        fqn.endswith('.header')
        or fqn.endswith('.txt')
        or fqn.endswith('.cat')
    ):
        meta['type'] = 'text/plain'
    elif fqn.endswith('.csv'):
        meta['type'] = 'text/csv'
    elif fqn.endswith('.gif'):
        meta['type'] = 'image/gif'
    elif fqn.endswith('.png'):
        meta['type'] = 'image/png'
    elif fqn.endswith('.jpg'):
        meta['type'] = 'image/jpeg'
    elif fqn.endswith('tar.gz'):
        meta['type'] = 'application/x-tar'
    elif fqn.endswith('h5') or fqn.endswith('hdf5'):
        meta['type'] = 'application/x-hdf5'
    else:
        meta['type'] = 'application/fits'
    logging.debug(meta)
    return meta


def get_file_size(fqn):
    """Get a file size on disk.

    :param fqn: Fully-qualified name of the file for which to get the size
    """
    s = os.stat(fqn)
    return s.st_size


def get_now():
    """So that now can be mocked.
    :return timezone-naive datetime.datetime
    """
    return datetime.now()


def get_version(collection):
    """A common implementation to retrieve a pipeline version."""
    try:
        application = f'{collection.lower()}2caom2'
        v = version(application)
    except PackageNotFoundError as e:
        # for BRITE-Constellation
        application = f'{collection.split("-")[0].lower()}2caom2'
        try:
            v = version(application)
        except PackageNotFoundError as e2:
            logging.warning(f'No version found for {application}')
            v = '0.0.0'
    return f'{application}/{v}'


def create_dir(dir_name):
    """Create the working area if it does not already exist."""
    if os.path.exists(dir_name):
        if not os.path.isdir(dir_name):
            raise CadcException(f'{dir_name} is not a directory.')
        if not os.access(dir_name, os.W_OK | os.X_OK):
            os.chmod(dir_name, stat.S_IRWXU)
            if not os.access(dir_name, os.W_OK | os.X_OK):
                raise CadcException(f'{dir_name} is not writeable.')
    else:
        os.makedirs(dir_name, mode=0o775)


def decompose_uri(uri):
    """
    Returns a scheme, path (maybe an archive), and a file name from the
    command line.
    """
    try:
        temp = uri.split(':', 1)
        scheme = temp[0]
        temp1 = temp[1].rsplit('/', 1)
        path = temp1[0]
        file_name = temp1[1]
        return scheme, path, file_name
    except Exception as e:
        logging.debug(
            f'URI {uri} caused error {e}. Expected scheme:path/FILE_NAME'
        )
        logging.debug(traceback.format_exc())
        raise CadcException(f'Expected scheme:path/FILE_NAME. Got {uri}.')


def check_param(param, param_type):
    """Generic code to check if a parameter is not None, and is of the
    expected type.
    """
    if param is None or not isinstance(param, param_type):
        raise CadcException(
            f'Parameter {param} failed check for {param_type}'
        )


def read_csv_file(fqn):
    """Read a csv file.

    :returns a list of lists.
    """
    results = []
    try:
        with open(fqn) as csv_file:
            reader = csv.reader(csv_file)
            for row in reader:
                if row[0].startswith('#'):
                    continue
                results.append(row)
    except Exception as e:
        logging.error(f'Could not read from csv file {fqn}')
        logging.debug(traceback.format_exc())
        raise CadcException(e)
    return results


def write_obs_to_file(obs, fqn):
    """Common code to write a CAOM Observation to a file."""
    ow = ObservationWriter()
    ow.write(obs, fqn)


def read_obs_from_file(fqn):
    """Common code to read a CAOM Observation from a file."""
    if not os.path.exists(fqn):
        raise CadcException(f'Could not find {fqn}')
    reader = ObservationReader(False)
    return reader.read(fqn)


def read_from_file(fqn):
    """Common code to read from a text file. Mostly to make it easy to
    mock.
    """
    if not os.path.exists(fqn):
        raise CadcException(f'Could not find {fqn}')
    with open(fqn) as f:
        return f.readlines()


def write_to_file(fqn, content):
    """Common code to write to a fully-qualified file name. Mostly to make
    it easy to mock.
    """
    try:
        with open(fqn, 'w') as f:
            f.write(content)
    except Exception:
        logging.error(f'Could not write file {fqn}')
        raise CadcException(f'Could not write file {fqn}')


def update_typed_set(typed_set, new_set):
    """Common code to remove all the entries from an existing set, and
    then replace those entries with a new set.
    """
    # remove the previous values
    while len(typed_set) > 0:
        typed_set.pop()
    typed_set.update(new_set)


def format_time_for_query(from_time):
    length = len(datetime.now().strftime('%Y-%m-%dT%H:%M:%S'))
    return datetime.strptime(from_time[:length], '%Y-%m-%dT%H:%M:%S')


def get_keyword(headers, keyword):
    result = headers[0].get(keyword)
    if result is None and len(headers) > 1:
        result = headers[1].get(keyword)
    return result


def get_artifact_metadata(
    fqn, product_type, release_type, uri=None, artifact=None
):
    """
    Build or update artifact content metadata using the CAOM2 objects, and
    with access to a file on disk.

    :param fqn: The fully-qualified name of the file on disk, for which an
        Artifact is being created or updated.
    :param product_type: which ProductType enumeration value
    :param release_type: which ReleaseType enumeration value
    :param uri: mandatory if creating an Artifact, a URI of the form
        scheme:ARCHIVE/file_name
    :param artifact: use when updating an existing Artifact instance

    :return: the created or updated Artifact instance, with the
        content_* elements filled in.
    """
    local_meta = get_file_meta(fqn)
    md5uri = ChecksumURI(f'md5:{local_meta["md5sum"]}')
    if artifact is None:
        if uri is None:
            raise CadcException('Cannot build an Artifact without a URI.')
        return Artifact(
            uri,
            product_type,
            release_type,
            local_meta['type'],
            local_meta['size'],
            md5uri,
        )
    else:
        artifact.product_type = product_type
        artifact.content_type = local_meta['type']
        artifact.content_length = local_meta['size']
        artifact.content_checksum = md5uri
        artifact.release_type = release_type
        return artifact


def get_artifact_metadata_client(
    client,
    file_name,
    product_type,
    release_type,
    archive,
    uri=None,
    artifact=None,
):
    """
    Build or update artifact content metadata using the CAOM2 objects, and
    with access to a file at CADC.

    :param client: CadcDataClient instance
    :param file_name: The name of the file in CADC storage, for which an
        Artifact is being created or updated.
    :param product_type: which ProductType enumeration value
    :param release_type: which ReleaseType enumeration value
    :param archive: str narrows down CADC storage location
    :param uri: mandatory if creating an Artifact, a URI of the form
        scheme:ARCHIVE/file_name
    :param artifact: use when updating an existing Artifact instance

    :return: the created or updated Artifact instance, with the
        content_* elements filled in.
    """
    meta = get_cadc_meta_client(client, archive, file_name)
    md5uri = ChecksumURI(f'md5:{meta.get("md5sum")}')
    if artifact is None:
        if uri is None:
            raise CadcException('Cannot build an Artifact without a URI.')
        return Artifact(
            uri,
            product_type,
            release_type,
            meta.get('type'),
            to_int(meta.get('size')),
            md5uri,
        )
    else:
        artifact.product_type = product_type
        artifact.content_type = meta.get('type')
        artifact.content_length = to_int(meta.get('size'))
        artifact.content_checksum = md5uri
        return artifact


def convert_to_days(exposure_time):
    """
    Converts from seconds to days.

    :param exposure_time:
    :return:
    """
    return exposure_time / (24.0 * 3600.0)


def sizeof(x):
    """Encapsulate returning the memory size in bytes."""
    return sys.getsizeof(x)


def build_uri(archive, file_name, scheme='ad'):
    """One location to keep the syntax for an Artifact URI."""
    return f'{scheme}:{archive}/{file_name}'


def query_endpoint(url, timeout=20):
    """Return a response for an endpoint. Caller needs to call 'close'
    on the response.
    """

    # Open the URL and fetch the JSON document for the observation
    session = requests.Session()
    retries = 10
    retry = Retry(
        total=retries, read=retries, connect=retries, backoff_factor=0.5
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.debug(traceback.format_exc())
        raise CadcException(f'Endpoint {url} failure {str(e)}')


def get_endpoint_session(retries=10, backoff_factor=0.5):
    session = requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session


def query_endpoint_session(url, session, timeout=20):
    """Return a response for an endpoint. Caller needs to call 'close'
    on the response.
    """

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response
    except Exception as e:
        logging.debug(traceback.format_exc())
        raise CadcException(f'Endpoint {url} failure {str(e)}')


def read_as_yaml(fqn):
    """Read and return YAML content of 'fqn'."""
    try:
        logging.debug(f'Begin read_as_yaml for {fqn}.')
        with open(fqn) as f:
            data_map = yaml.safe_load(f)
            logging.debug('End read_as_yaml.')
            return data_map
    except (yaml.scanner.ScannerError, FileNotFoundError) as e:
        logging.error(e)
        return None


def write_as_yaml(content, fqn):
    """Write 'content' to 'fqn' as YAML."""
    try:
        logging.debug(f'Begin write_as_yaml for {fqn}.')
        with open(fqn, 'w') as f:
            yaml.dump(content, f, default_flow_style=False)
            logging.debug('End write_as_yaml.')
    except Exception as e:
        logging.debug(traceback.format_exc())
        logging.error(e)


def load_module(module, command_name):
    """
    Add code to the execution environment of the interpreter.

    :param module the fully-qualified path name to the source code.
    :param command_name what will be executed within the module.
    """
    mname = os.path.basename(module)
    if '.' in mname:
        # remove extension from the provided name
        mname = mname.split('.')[0]
    pname = os.path.dirname(module)
    sys.path.append(pname)
    try:
        logging.debug(f'Looking for {command_name} in {module}.')
        result = importlib.import_module(mname)
        if not hasattr(result, command_name):
            raise CadcException(
                f'Could not find command {command_name} in module {module}.'
            )
    except ImportError as e:
        logging.debug(f'Looking for {mname} in {pname}')
        raise e
    return result


def _parse_plus_some_formats(from_value):
    """
    Use dateutil.parser.parse to transform string values to datetime values, except in those cases that parse
    doesn't understand.

    The formats that are not understood are valid values from a collection somewhere.
    :param from_value: str
    :return: datetime
    """
    tzinfos = None
    if 'HST' in from_value:
        tzinfos = {'HST': tz.gettz('HST')}
    result = None
    try:
        result = parser.parse(from_value, tzinfos=tzinfos)
    except ValueError as e1:
        for fmt in [
            '%H:%M:%S',
            '%Y/%m/%d,%H:%M:%S',
            '%Y_%m_%dT%H_%M_%S.%f',
            '%Y-%m-%dHST%H:%M:%S',
            '%Y%b%d',
        ]:
            try:
                result = datetime.strptime(from_value, fmt)
                if 'HST' in fmt:
                    # change timezone from HST to UTC - add 10 hours, accurate enough implementation for the CFHT
                    # provenance.lastExecuted value
                    result = result + timedelta(hours=10)
                break
            except ValueError as e2:
                pass
    return result


def make_datetime(from_value):
    """
    :param from_value a representation of time.
    :return a timezone-naive datetime value.
    """
    result = None
    if from_value is None:
        logging.info('make_datetime: input is None')
    else:
        if isinstance(from_value, datetime):
            result = from_value
        elif isinstance(from_value, str):
            result = _parse_plus_some_formats(from_value)
        elif isinstance(from_value, float):
            result = datetime.fromtimestamp(from_value)
        else:
            logging.error(f'make_datetime. Unexpected type {type(from_value)}. No conversion to datetime.')
    if result is not None and result.tzinfo is not None:
        # the astimezone fixes the offset, the replace removes the tzinfo
        result = result.astimezone(tz=None).replace(tzinfo=None)
    return result


def increment_time(this_dt, by_interval, unit='%M'):
    """
    Increment time by an interval. Times should be in datetime format, but
    a modest attempt is made to check for otherwise.

    :param this_dt: datetime
    :param by_interval: integer - e.g. 10, for a 10 minute increment
    :param unit: the formatting string, default is minutes
    :return: this_ts incremented by interval amount.
    """
    temp = None
    time_dt = make_datetime(this_dt)
    if time_dt is not None:
        if unit == '%M':
            temp = time_dt + timedelta(minutes=by_interval)
        else:
            raise NotImplementedError(f'Unexpected unit {unit}')
    return temp


def http_get(url, local_fqn):
    """Retrieve a file via http.

    :param url where the file can be found.
    :param local_fqn fully qualified name for where to file the file
        locally.
    """
    try:
        with requests.get(url, stream=True, timeout=10) as r:
            r.raise_for_status()
            with open(local_fqn, 'wb') as f:
                for chunk in r.iter_content(chunk_size=READ_BLOCK_SIZE):
                    f.write(chunk)
            length = to_int(r.headers.get('Content-Length'))
            if length is not None:
                file_meta = get_file_meta(local_fqn)
                if file_meta['size'] != length:
                    raise CadcException(
                        f'Could not retrieve {local_fqn} from {url}. File '
                        f'size error.'
                    )
            checksum = r.headers.get('Content-Checksum')
            if checksum is not None:
                file_meta = get_file_meta(local_fqn)
                if file_meta['md5sum'] != checksum:
                    raise CadcException(
                        f'Could not retrieve {local_fqn} from {url}. File '
                        f'checksum error.'
                    )
        if not os.path.exists(local_fqn):
            raise CadcException(
                f'Retrieve failed. {local_fqn} does not exist.'
            )
    except requests.exceptions.HTTPError as e:
        logging.debug(traceback.format_exc())
        raise CadcException(
            f'Could not retrieve {local_fqn} from {url}. Failed with {e}'
        )


@dataclass
class FileMeta:
    """The bits of information about a file that are used to decide whether
    or not to transfer, and whether or not a transfer was successful."""

    f_size: int
    md5sum: str


def _get_file_info(storage_name, cadc_client):
    """
    Retrieve metadata for a single file. This implementation uses a
    vos.Client.

    This is a very bad implementation, but there is no information on what
    might be a better one.

    :param storage_name: Artifact URI
    :param cadc_client:
    :return:
    """
    node = cadc_client.get_node(storage_name, limit=None, force=False)
    f_size = node.props.get('length')
    f_md5sum = node.props.get('MD5')
    return FileMeta(f_size, f_md5sum)


def query_tap(query_string, proxy_fqn, resource_id, timeout=10):
    """
    :param query_string ADQL
    :param proxy_fqn proxy file location, credentials for the query
    :param resource_id which tap service to query
    :param timeout time in minutes, tap_client gives up after that
    :returns an astropy votable instance."""

    logging.debug(
        f'query_tap: execute query {query_string} against {resource_id}'
    )
    subject = net.Subject(certificate=proxy_fqn)
    tap_client = CadcTapClient(subject, resource_id=resource_id)
    buffer = io.StringIO()
    tap_client.query(query_string, output_file=buffer, data_only=True, response_format='tsv', timeout=timeout)
    return Table.read(buffer.getvalue().split('\n'), format='ascii.tab')


def query_tap_pandas(query_string, client, timeout=10):
    """
    Return TAP query results as a Pandas Dataframe.

    :param query_string: query to execute
    :param client: CadcTapClient instance, pointed to a service that understands the query
    :param timeout: in minutes

    :return: Dataframe with query results
    """
    buffer = io.StringIO()
    client.query(query_string, output_file=buffer, data_only=True, response_format='tsv', timeout=timeout)
    return Table.read(buffer.getvalue().split('\n'), format='ascii.tab').to_pandas()


def reverse_lookup(value_to_find, in_dict):
    """
    Use a generator expression to do a reverse-lookup in a dictionary.
    :param value_to_find value
    :param in_dict dictionary that might have a key for the value
    """
    exp = (key for key, value in in_dict.items() if value == value_to_find)
    result = None
    for entry in exp:
        result = entry
        break
    return result


def find_missing(compare_this, to_this):
    """
    :param compare_this: list
    :param to_this: list
    :return: list of elements from to_this not in compare_this
    """
    return list(set(compare_this).difference(to_this))


class ValueRepairCache(Cache):
    """Use a cache file to repair metadata values.

    The cache file is identified in the config.yml file.

    The cache file contents should look like:
    value_repair:  # must have this value
      observation.type:  # a fully-qualified caom2 entity.attribute value
        dark: DARK       # a key: value pair

    There can be multiple entries per attribute:
      e.g. observation.type:
             dark: DARK
             Dark: DARK
             object: OBJECT

    The key and the value are treated as regular expressions, with the
    following exceptions:

    Use the string value 'none' to set an attribute to None:
      e.g. observation.type:
             dark: none

    Use the string value 'none' to set un-set attributes to a value:
      e.g. observation.type:
             none: DARK

    Use the string value 'any' to set all values of an attribute to a single
    value:
      e.g. observation.type:
             any: DARK

    Caveats:

    Some CAOM2 attributes are not assignable. These attributes cannot be
    repaired using this class.

    List content cannot be repaired using this class.

    Values that are originally None are un-changed, unless 'none' is provided
    as the key.

    """

    VALUE_REPAIR = 'value_repair'

    def __init__(self):
        super().__init__()
        self._value_repair = self.get_from(ValueRepairCache.VALUE_REPAIR)
        self._key = None
        self._values = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def repair(self, observation):
        try:
            for key, values in self._value_repair.items():
                self._logger.debug(f'Checking for {key}')
                self._key = key
                self._values = values
                bits = key.split('.')[1:]
                if key.startswith('observation'):
                    self._recurse(observation, 'observation', bits)
                elif key.startswith('plane'):
                    for plane in observation.planes.values():
                        self._recurse(plane, 'plane', bits)
                elif key.startswith('artifact'):
                    for plane in observation.planes.values():
                        for artifact in plane.artifacts.values():
                            self._recurse(artifact, 'artifact', bits)
                elif key.startswith('part'):
                    for plane in observation.planes.values():
                        for artifact in plane.artifacts.values():
                            for part in artifact.parts.values():
                                self._recurse(part, 'part', bits)
                elif key.startswith('chunk'):
                    for plane in observation.planes.values():
                        for artifact in plane.artifacts.values():
                            for part in artifact.parts.values():
                                for chunk in part.chunks:
                                    self._recurse(chunk, 'chunk', bits)
                else:
                    raise CadcException(f'Unexpected repair key {key}.')
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            raise CadcException(e)

    def _recurse(self, entity, entity_name, bits):
        attribute_name = bits[0]
        if entity is None:
            self._logger.debug(f'Entity {entity_name} is None, so not looking for repairs.')
        elif hasattr(entity, attribute_name):
            if len(bits) == 1:
                self._repair_attribute(entity, attribute_name)
            else:
                new_entity = getattr(entity, attribute_name)
                self._recurse(new_entity, attribute_name, bits[1:])
        else:
            self._logger.warning(f'No attribute {entity_name}.{attribute_name} found to repair.')

    def _repair_attribute(self, entity, attribute_name):
        try:
            attribute_value = getattr(entity, attribute_name)
            attribute_repr = (
                attribute_value.value
                if isinstance(attribute_value, Enum)
                else attribute_value
            )
            if (
                'any' in self._values.keys()
                or 'none' in self._values.keys()
                or attribute_repr in self._values.keys()
            ):
                for original, fix in self._values.items():
                    if attribute_value is None and original != 'none':
                        self._logger.info(
                            f'{attribute_name} value is None. This class only '
                            f'repairs values.'
                        )
                    else:
                        fixed = self._fix(
                            entity,
                            attribute_name,
                            attribute_value,
                            original,
                            fix,
                        )
                        if (
                            fixed is None
                            or fixed == fix
                            or fixed != attribute_value
                        ):
                            break
        except Exception as e:
            self._logger.debug(traceback.format_exc())
            raise CadcException(e)

    def _fix(self, entity, attribute_name, attribute_value, original, fix):
        if fix == 'none':
            setattr(entity, attribute_name, None)
            self._logger.info(f'Repair {self._key} from {original} to None')
            fixed = None
        elif original == 'any':
            setattr(entity, attribute_name, fix)
            self._logger.info(
                f'Repair {self._key} from {attribute_value} to {fix}'
            )
            fixed = fix
        else:
            if attribute_value is None:
                setattr(entity, attribute_name, fix)
                fixed = fix
            else:
                attribute_value_type = type(attribute_value)
                fixed = re.sub(str(original), str(fix), str(attribute_value))
                setattr(entity, attribute_name, attribute_value_type(fixed))
        new_value = getattr(entity, attribute_name)
        if attribute_value != new_value:
            self._logger.info(
                f'Repair {self._key} from {attribute_value} to {fixed}'
            )
        return fixed
