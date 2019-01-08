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
package org.neo4j.test;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

public final class DatabaseFunctions
{
    private DatabaseFunctions()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static Function<GraphDatabaseService,Node> createNode()
    {
        return GraphDatabaseService::createNode;
    }

    public static Function<Node,Node> addLabel( Label label )
    {
        return node ->
        {
            node.addLabel( label );
            return node;
        };
    }

    public static Function<Node,Node> setProperty( String propertyKey, Object value )
    {
        return node ->
        {
            node.setProperty( propertyKey, value );
            return node;
        };
    }

    public static Function<GraphDatabaseService,Void> index( Label label, String propertyKey )
    {
        return graphDb ->
        {
            graphDb.schema().indexFor( label ).on( propertyKey ).create();
            return null;
        };
    }

    public static Function<GraphDatabaseService,Void> uniquenessConstraint( Label label, String propertyKey )
    {
        return graphDb ->
        {
            graphDb.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
            return null;
        };
    }

    public static Function<GraphDatabaseService,Void> awaitIndexesOnline( long timeout, TimeUnit unit )
    {
        return graphDb ->
        {
            graphDb.schema().awaitIndexesOnline( timeout, unit );
            return null;
        };
    }
}
