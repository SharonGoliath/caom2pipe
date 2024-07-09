# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2023.                            (c) 2023.
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

import re

from dataclasses import dataclass
from treelib import Tree
from typing import Callable

from caom2pipe.data_source_composable import IncrementalDataSource, StateRunnerMeta
from caom2pipe.manage_composable import make_datetime, query_endpoint_session


__all__ = ['HtmlFilter', 'HtmlFilteredPagesTemplate', 'HttpDataSource']


@dataclass
class HtmlFilter:
    """
    The HttpsDataSource scrapes hierarchical HTML pages for links to files to be able to harvest them incrementally.
    Some timestamps on the pages with links to be followed do not get updated in concert with pages lower in the
    hierarchy. Use the ignore_datetime flag to identify hierarchy levels where all links found should be followed.
    """
    # the filter function to be called
    fn: Callable
    # if True, ignore the effect of the datetime check for time-boxing purposes
    ignore_datetime: bool


class HtmlFilteredPagesTemplate:
    """
    Example HTML Page Structure, where each page has time-stamped URLs that should be followed. Eventually the URLs
    will point to files for archiving at CADC.

    TOP
     | - YEAR (many, e.g. 2023)
           | - DAY (e.g. 364)
    """

    @staticmethod
    def filter_never(ignore_href):
        return True

    always_true_filter = HtmlFilter(filter_never, False)  # False - always check

    def __init__(self, config):
        temp = '|'.join(f'{ii}$' for ii in config.data_source_extensions).replace('.', '\\.')

        def filter_by_extensions(href):
            return re.search(temp, href)

        # two filters that might be useful elsewhere
        self._always_true_filter = HtmlFilter(HtmlFilteredPagesTemplate.filter_never, True)  # True - least restrictive
        self._file_filter = HtmlFilter(filter_by_extensions, False)  # False - only new files

    def add_children(self, to_node, in_tree, new_entries):
        """Use the template Tree to figure out which filter function applies to which URL and the HTML page content."""
        if in_tree.parent(to_node.identifier).is_root():
            template_filter = self._always_true_filter
        else:
            template_filter = self._file_filter
        for url in new_entries:
            in_tree.create_node(url, parent=to_node.identifier, data=template_filter)

    def first_filter(self):
        """"""
        return self._always_true_filter

    def is_leaf(self, url_tree, url_node):
        return url_tree.depth(url_node) == 2


class HttpDataSource(IncrementalDataSource):
    """
    Support incremental harvesting from hierarchical HTML pages that list URLs with timestamps.
    """

    def __init__(self, config, start_key, html_filters, session):
        """
        :param config: manage_composable.Config instance
        :param html_filters: list of HtmlFilter instances to filter an html string for additional time-boxed
          href values.
        :param session: requests.Session instance
        :param start_key: str, the key to use for looking up the bookmarked start time in a state.yml file.
        """
        super().__init__(config, start_key)
        self._html_filters = html_filters
        self._session = session
        # the keys are URLs, the values are datetimes
        # the keys are URLs because the file names they represent are expected to be unique, while the datetimes are
        # not necessarily unique.
        # self._todo_list is the list of work obtained from the data source, self._work is the list of work organized
        # for execution by the run_composable classes.
        self._todo_list = dict()
        self._data_sources = config.data_sources
        self._tree = Tree()
        self._tree.create_node('ROOT', 'root')
        self._tree.create_node(tag=start_key, parent='root', data=html_filters.first_filter())

    @property
    def html_filters(self):
        return self._html_filters

    def _descend_html_hierarchy(self, node):
        """
        This works for html pages with links that have timestamps. Eventually, the pages are expected to list files,
        also with timestamps.

        This will not work for other page layouts or hierarchies.

        :param node: Node, contains information on how to scrape a URL
        :return: a dict, keys are URLs, values are timestamps.
        """
        self._logger.debug(
            f'Begin _descend_html_hierarchy with {node.tag} for {len(self._tree.children(node.identifier))} URLs'
        )
        for child_node in self._tree.children(node.identifier):
            response = None
            try:
                self._logger.info(f'Querying {child_node.tag} for time-stamped entries.')
                response = query_endpoint_session(child_node.tag, self._session)
                response.raise_for_status()
                if response is None:
                    self._logger.warning(f'No response from {child_node.tag}.')
                else:
                    result = self._parse_html_string(child_node, response.text)
                    if self._html_filters.is_leaf(self._tree, child_node):
                        self._todo_list = dict(self._todo_list, **result)
                    else:
                        self._html_filters.add_children(child_node, self._tree, result)
                        self._descend_html_hierarchy(child_node)
            finally:
                if response is not None:
                    response.close()
        self._logger.debug(f'End _descend_html_hierarchy with {node.tag}')

    def _initialize_end_dt(self):
        self._logger.debug('Begin _initialize_end_dt')
        self._descend_html_hierarchy(self._tree.get_node('root'))
        # sort the work to be done by timestamp
        sorted_todo_list = sorted(self._todo_list.items(), key=lambda x: x[1])
        for url, dt in sorted_todo_list:
            entry = StateRunnerMeta(url, dt)
            self._work.append(entry)
        if len(self._todo_list) > 0:
            self._logger.info(f'Found {len(self._work)} total records.')
            self._end_dt = max(self._todo_list.values())
        else:
            # the default implementation assumes a remote data source, so the time-box end-point only advances if
            # the remote data source advances
            self._logger.warning(f'Found no records. Setting end date to {self._start_dt}.')
            self._end_dt = self._start_dt
        # only need the sorted copy
        self._todo_list = dict()
        self._tree = Tree()  # TODO I wonder if this works?
        self._logger.debug('End _initialize_end_dt')

    def _parse_html_string(self, node, html_string):
        """
        Filter URLs from the HTML at an HTTP page, according to provided filters.

        :param node: Node, has information to filter URLs from HTML
        :param html_string: HTML from a URL
        :return: dict, keys are URLs, values are datetimes
        """
        # local import to limit the number of Docker images that need to pip install bs4
        from bs4 import BeautifulSoup
        if node.data.ignore_datetime:
            msg = ', will ignore timestamps.'
        else:
            msg = f', will return links later than {self._start_dt}.'
        self._logger.debug(f'Begin _parse_html_string from {node.tag} with {node.data.fn.__name__}{msg}')
        result = {}
        soup = BeautifulSoup(html_string, features='lxml')
        hrefs = soup.find_all('a')
        for href in hrefs:
            href_name = href.get('href')
            if node.data.fn(href_name):
                dt_str = href.next_element.next_element.string
                if dt_str is None:
                    continue
                dt_str_bits = dt_str.split()
                if len(dt_str_bits) >= 2:
                    dt = make_datetime(f'{dt_str_bits[0]} {dt_str_bits[1]}')
                    if dt is None:
                        self._logger.debug(f'Skip html date parsing for {dt_str_bits}.')
                        continue
                    if dt >= self._start_dt or node.data.ignore_datetime:
                        if href_name.startswith('http'):
                            temp = href_name
                        else:
                            if node.tag.endswith('/'):
                                temp = f'{node.tag}{href_name}'
                            else:
                                temp = f'{node.tag}/{href_name}'
                            self._logger.debug(f'Found url: {temp}')
                        result[temp] = dt
                else:
                    self._logger.debug(f'skipping {dt_str_bits}')
        self._logger.debug(f'End _parse_html_string from {node.tag} with {node.data.fn.__name__}')
        return result

