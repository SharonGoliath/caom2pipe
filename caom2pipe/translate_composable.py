# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2020.                            (c) 2020.
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

from caom2 import TypedList, Chunk
from caom2utils.parsers import FitsParser


__all__ = ['add_headers_to_obs_by_blueprint']


def add_headers_to_obs_by_blueprint(obs, headers, blueprint, uri, product_id):
    """
    Common code that puts together knowledge of the blueprint and the
    FitsParser to add information to an Observation.

    :param obs Observation to be added to
    :param headers astropy FITS headers with in-coming metadata
    :param blueprint caom2utils.ObsBlueprint instance to map from the
        metadata to the CAOM structure
    :param uri Artifact URI to add to Observation instance
    :param product_id Plane identifier, to know which plane to add

    """
    parser = FitsParser(headers, blueprint, uri)
    parser.augment_observation(obs, uri, product_id)

    # re-home the chunk information to be consistent with accepted CAOM
    # patterns of part/chunk relationship - i.e. part 0 never has chunks
    # if the file being represented has extensions. This
    # relationship gets missed when only using the headers[1:], so
    # shift all the chunks up by one, and set the 0th to no chunks
    for plane in obs.planes.values():
        if plane.product_id != product_id:
            continue
        for artifact in plane.artifacts.values():
            if artifact.uri == uri and len(artifact.parts) > 1:
                first_index = None
                prev_chunks = None
                for key, value in artifact.parts.items():
                    if first_index is None:
                        first_index = key
                        prev_chunks = value.chunks
                        continue
                    else:
                        temp = value.chunks
                        value.chunks = prev_chunks
                        prev_chunks = temp
                artifact.parts['0'].chunks = TypedList(
                    Chunk,
                )
