/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import org.junit.Test;

import java.net.URI;
import java.util.Map;

import org.neo4j.helpers.AdvertisedSocketAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DiscoveryRepresentationTest
{
    @Test
    public void shouldCreateAMapContainingDataAndManagementURIs()
    {
        String managementUri = "/management";
        String dataUri = "/data";
        AdvertisedSocketAddress boltAddress = new AdvertisedSocketAddress( "localhost", 7687 );
        DiscoveryRepresentation dr = new DiscoveryRepresentation( managementUri, dataUri, boltAddress );

        Map<String,Object> mapOfUris = RepresentationTestAccess.serialize( dr );

        Object mappedManagementUri = mapOfUris.get( "management" );
        Object mappedDataUri = mapOfUris.get( "data" );
        Object mappedBoltUri = mapOfUris.get( "bolt" );

        assertNotNull( mappedManagementUri );
        assertNotNull( mappedDataUri );
        assertNotNull( mappedBoltUri );

        URI baseUri = RepresentationTestBase.BASE_URI;

        assertEquals( mappedManagementUri.toString(), Serializer.joinBaseWithRelativePath( baseUri, managementUri ) );
        assertEquals( mappedDataUri.toString(), Serializer.joinBaseWithRelativePath( baseUri, dataUri ) );
        assertEquals( mappedBoltUri.toString(), toBoltUri( boltAddress ) );
    }

    private String toBoltUri( AdvertisedSocketAddress boltAddress )
    {
        return "bolt://" + boltAddress.getHostname() + ":" + boltAddress.getPort();
    }
}
