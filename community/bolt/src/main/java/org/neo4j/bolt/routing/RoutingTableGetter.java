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
package org.neo4j.bolt.routing;

import java.util.concurrent.CompletableFuture;

import org.neo4j.bolt.runtime.statemachine.StatementProcessor;
import org.neo4j.values.virtual.MapValue;

/**
 * Interface which provides the routing table given a routing context and a database name.
 */
public interface RoutingTableGetter
{
    /**
     * Retrieves the routing table
     *
     * @param statementProcessor The statement processor which will be used to get the routing information.
     * @param routingContext Meta information used by the routing procedure to create the routing table.
     * @param databaseName The name of the database from the routing table will be get.
     * @return A promise of a routing table
     */
    CompletableFuture<MapValue> get( StatementProcessor statementProcessor, MapValue routingContext, String databaseName );
}
