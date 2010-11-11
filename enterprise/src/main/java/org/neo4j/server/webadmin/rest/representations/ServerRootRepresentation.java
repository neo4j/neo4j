/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.webadmin.rest.representations;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.server.rest.domain.Representation;
import org.neo4j.server.webadmin.rest.AdvertisableService;

public class ServerRootRepresentation implements Representation {

    private static final String SERVER_PATH = "server/console";
    private HashMap<String, String> services = new HashMap<String, String>();

    public ServerRootRepresentation(URI baseUri, AdvertisableService ... advertisableServices) {
        for(AdvertisableService svc : advertisableServices) {
            services.put(svc.getName(), baseUri.toString() + "/" +svc.getServerPath());
        }
    }

    public Map<String,String> serialize() {
        return services;
    }

    public String getServerPath() {
        return SERVER_PATH;
    }
}
