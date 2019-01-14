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

import org.neo4j.helpers.AdvertisedSocketAddress;

public class DiscoveryRepresentation extends MappingRepresentation
{

    private static final String DATA_URI_KEY = "data";
    private static final String MANAGEMENT_URI_KEY = "management";
    private static final String BOLT_URI_KEY = "bolt";
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";
    private final String managementUri;
    private final String dataUri;
    private final AdvertisedSocketAddress boltAddress;

    public DiscoveryRepresentation( String managementUri, String dataUri, AdvertisedSocketAddress boltAddress )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.managementUri = managementUri;
        this.dataUri = dataUri;
        this.boltAddress = boltAddress;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( MANAGEMENT_URI_KEY, managementUri );
        serializer.putRelativeUri( DATA_URI_KEY, dataUri );
        serializer.putAbsoluteUri( BOLT_URI_KEY, "bolt://" + boltAddress.getHostname() + ":" + boltAddress.getPort() );
    }
}
