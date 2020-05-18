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
package org.neo4j.fabric.bootstrap;

import org.neo4j.bolt.v41.messaging.RoutingContext;

/**
 * A collection of methods that allow overriding default behaviour to make some aspects of Fabric testable.
 * An override is injected using thread local values, in order to avoid interference between tests executed in parallel.
 */
public class TestOverrides
{
    public static final InheritableThreadLocal<Boolean> MULTI_GRAPH_EVERYWHERE = new InheritableThreadLocal<>();
    public static final InheritableThreadLocal<RoutingContext> ROUTING_CONTEXT = new InheritableThreadLocal<>();

    public static boolean multiGraphCapabilitiesEnabledForAllDatabases( boolean originalValue )
    {
        var overriddenValue = MULTI_GRAPH_EVERYWHERE.get();

        if ( overriddenValue != null )
        {
            return overriddenValue;
        }

        return originalValue;
    }

    public static RoutingContext routingContext( RoutingContext originalValue )
    {
        var overriddenValue = ROUTING_CONTEXT.get();

        if ( overriddenValue != null )
        {
            return overriddenValue;
        }

        return originalValue;
    }
}
