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
package org.neo4j.server.rest.repr;


import java.net.URI;

public class DiscoveryRepresentation extends MappingRepresentation {

    private static final String DATA_URI_KEY = "data";
    private static final String MANAGEMENT_URI_KEY = "management";
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";
    private final java.net.URI managementUri;
    private final java.net.URI dataUri;

    public DiscoveryRepresentation(URI managementUri, URI dataUri) {
        super(DISCOVERY_REPRESENTATION_TYPE);
        this.managementUri = managementUri;
        this.dataUri = dataUri;
    }
    
    @Override
    protected void serialize(MappingSerializer serializer) {
        serializer.putString(MANAGEMENT_URI_KEY, managementUri.toString());
        serializer.putString(DATA_URI_KEY, dataUri.toString());
    }

}
