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
package org.neo4j.server.rest.discovery;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.server.configuration.ServerSettings;

import static org.neo4j.server.rest.discovery.DiscoverableURIs.Precedence.NORMAL;

public class CommunityDiscoverableURIs
{
    /**
     * URIs exposed at the root HTTP endpoint, to help clients discover the rest of the service.
     */
    public static DiscoverableURIs communityDiscoverableURIs( Config config, ConnectorPortRegister portRegister )
    {
        return new DiscoverableURIs.Builder()
                .add( "data", config.get( ServerSettings.rest_api_path ).getPath() + "/", NORMAL )
                .add( "management", config.get( ServerSettings.management_api_path ).getPath() + "/", NORMAL )
                .addBoltConnectorFromConfig( "bolt", "bolt", config, ServerSettings.bolt_discoverable_address, portRegister )
                .build();
    }
}
