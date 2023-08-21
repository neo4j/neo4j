/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.repr;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.neo4j.server.rest.repr.Serializer.joinBaseWithRelativePath;

import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.rest.discovery.ServerVersionAndEdition;

class DiscoveryRepresentationTest {
    @Test
    void shouldCreateAMapContainingDataAndManagementURIs() {
        var baseUri = RepresentationTestBase.BASE_URI;
        var managementUri = "/management";
        var dataUri = "/data";
        var config = Config.defaults(BoltConnector.enabled, true);
        var dr = new DiscoveryRepresentation(
                new DiscoverableURIs.Builder(null)
                        .addEndpoint("management", managementUri)
                        .addEndpoint("data", dataUri)
                        .addBoltEndpoint(config, mock(ConnectorPortRegister.class))
                        .build()
                        .update(baseUri),
                mock(ServerVersionAndEdition.class),
                new AuthConfigRepresentation());

        var mapOfUris = RepresentationTestAccess.serialize(dr);

        var mappedManagementUri = mapOfUris.get("management");
        var mappedDataUri = mapOfUris.get("data");
        var mappedBoltUri = mapOfUris.get("bolt_direct");

        assertNotNull(mappedManagementUri);
        assertNotNull(mappedDataUri);
        assertNotNull(mappedBoltUri);

        assertEquals(joinBaseWithRelativePath(baseUri, managementUri), mappedManagementUri.toString());
        assertEquals(joinBaseWithRelativePath(baseUri, dataUri), mappedDataUri.toString());
        assertEquals("bolt://neo4j.org:7687", mappedBoltUri.toString());
    }

    @Test
    void shouldCreateAMapContainingServerVersionAndEditionInfo() {
        var serverInfo = new ServerVersionAndEdition("myVersion", "myEdition");
        var dr = new DiscoveryRepresentation(mock(DiscoverableURIs.class), serverInfo, new AuthConfigRepresentation());

        var mapOfUris = RepresentationTestAccess.serialize(dr);

        var version = mapOfUris.get("neo4j_version");
        var edition = mapOfUris.get("neo4j_edition");
        var authConfig = mapOfUris.get("auth_config");

        assertNotNull(version);
        assertNotNull(edition);

        assertEquals("myVersion", version.toString());
        assertEquals("myEdition", edition.toString());
        assertNull(authConfig); // No auth_config for community.
    }
}
