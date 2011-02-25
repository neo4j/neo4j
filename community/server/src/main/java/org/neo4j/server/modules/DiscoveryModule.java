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

import static org.neo4j.server.JAXRSHelper.listFrom;

import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.server.JAXRSHelper;
import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;

public class DiscoveryModule implements ServerModule {

    private static final Logger log = Logger.getLogger(DiscoveryModule.class);
    private static final String ROOT_PATH = "/";
    
    public Set<URI> start(NeoServerWithEmbeddedWebServer neoServer) {
        neoServer.getWebServer().addJAXRSPackages(listFrom(new String[] { Configurator.ROOT_DISCOVERY_REST_API_PACKAGE }), ROOT_PATH);
        log.info("Mounted discovery module at [%s]", ROOT_PATH);
        
        HashSet<URI> ownedUris = new HashSet<URI>();
        ownedUris.add(JAXRSHelper.generateUriFor(neoServer.baseUri(), ROOT_PATH));
        return ownedUris;
    }
    
    public void stop() {
        // Do nothing.
    }
}
