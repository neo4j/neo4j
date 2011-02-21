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

import static org.neo4j.server.JAXRSHelper.generateUriFor;
import static org.neo4j.server.JAXRSHelper.listFrom;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.server.NeoServerWithEmbeddedWebServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;

public class ManagementApiModule implements ServerModule {

    private static final String DEFAULT_WEB_ADMIN_REST_API_PATH = "/db/manage";

    private final Logger log = Logger.getLogger(ManagementApiModule.class);
    private NeoServerWithEmbeddedWebServer neoServer;

    public Set<URI> start(NeoServerWithEmbeddedWebServer neoServer) {
        this.neoServer = neoServer;
        try {
            neoServer.getWebServer().addJAXRSPackages(listFrom(new String[] { Configurator.WEB_ADMIN_REST_API_PACKAGE }), managementApiUri().toString());
            log.info("Mounted management API at [%s]", managementApiUri().toString());
            
            HashSet<URI> ownedUris = new HashSet<URI>();
            ownedUris.add(managementApiUri());
            return ownedUris;
        } catch (UnknownHostException e) {
            log.warn(e);
            return new HashSet<URI>();
        }
    }
    
    public void stop() {
        // Do nothing.
    }

    private URI managementApiUri() throws UnknownHostException {
        if (neoServer.getConfiguration().containsKey(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY)) {
            try {
                return new URI(neoServer.getConfiguration().getProperty(Configurator.WEB_ADMIN_PATH_PROPERTY_KEY).toString());
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
        log.warn("Could not establish the Webadmin API URI from configuration, defaulting to [%s]",
                generateUriFor(neoServer.baseUri(), DEFAULT_WEB_ADMIN_REST_API_PATH));
        return generateUriFor(neoServer.baseUri(), DEFAULT_WEB_ADMIN_REST_API_PATH);
    }
}
