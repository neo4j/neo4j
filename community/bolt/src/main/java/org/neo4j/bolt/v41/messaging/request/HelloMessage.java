/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v41.messaging.request;

import java.util.Map;

import org.neo4j.bolt.v41.messaging.RoutingContext;

public class HelloMessage extends org.neo4j.bolt.v3.messaging.request.HelloMessage
{
    public static final String ROUTING = "routing";
    private final RoutingContext routingContext;
    private final Map<String,Object> authToken;

    public HelloMessage( Map<String,Object> meta, RoutingContext routingContext, Map<String,Object> authToken )
    {
        super( meta );
        this.routingContext = routingContext;
        this.authToken = authToken;
    }

    @Override
    public Map<String,Object> authToken()
    {
        return authToken;
    }

    public RoutingContext routingContext()
    {
        return routingContext;
    }
}
