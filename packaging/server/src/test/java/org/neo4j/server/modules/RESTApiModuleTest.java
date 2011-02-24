/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.modules;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.Set;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.web.WebServer;

public class RESTApiModuleTest {
    @Test
    public void shouldRegisterASingleUri() throws Exception {
        WebServer webServer = mock(WebServer.class);

        NeoServerWithEmbeddedWebServer neoServer = mock(NeoServerWithEmbeddedWebServer.class);
        when(neoServer.baseUri()).thenReturn(new URI("http://localhost:7575"));
        when(neoServer.getWebServer()).thenReturn(webServer);

        Configuration config = new PropertiesConfiguration();
        String path = "/db/data";
        config.addProperty(Configurator.DATA_API_PATH_PROPERTY_KEY, path);

        when(neoServer.getConfiguration()).thenReturn(config);

        RESTApiModule module = new RESTApiModule();
        Set<URI> uris = module.start(neoServer);

        assertEquals(1, uris.size());
        assertEquals(path, uris.iterator().next().getPath());
    }
}
