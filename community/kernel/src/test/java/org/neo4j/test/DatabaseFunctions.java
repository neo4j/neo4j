/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

public class DatabaseFunctions
{
    public static AlgebraicFunction<GraphDatabaseService, Node> createNode()
    {
        return new AlgebraicFunction<GraphDatabaseService, Node>()
        {
            @Override
            public Node apply( GraphDatabaseService graphDb )
            {
                return graphDb.createNode();
            }
        };
    }

    public static AlgebraicFunction<Node, Node> addLabel( final Label label )
    {
        return new AlgebraicFunction<Node, Node>()
        {
            @Override
            public Node apply( Node node )
            {
                node.addLabel( label );
                return node;
            }
        };
    }

    public static AlgebraicFunction<Node, Node> setProperty( final String propertyKey, final Object value )
    {
        return new AlgebraicFunction<Node, Node>()
        {
            @Override
            public Node apply( Node node )
            {
                node.setProperty( propertyKey, value );
                return node;
            }
        };
    }

    public static AlgebraicFunction<GraphDatabaseService, Void> index(
            final Label label, final String propertyKey )
    {
        return new AlgebraicFunction<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService graphDb )
            {
                graphDb.schema().indexFor( label ).on( propertyKey ).create();
                return null;
            }
        };
    }

    public static AlgebraicFunction<GraphDatabaseService, Void> uniquenessConstraint(
            final Label label, final String propertyKey )
    {
        return new AlgebraicFunction<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService graphDb )
            {
                graphDb.schema().constraintFor( label ).assertPropertyIsUnique( propertyKey ).create();
                return null;
            }
        };
    }

    public static AlgebraicFunction<GraphDatabaseService, Void> awaitIndexesOnline(
            final long timeout, final TimeUnit unit )
    {
        return new AlgebraicFunction<GraphDatabaseService, Void>()
        {
            @Override
            public Void apply( GraphDatabaseService graphDb )
            {
                graphDb.schema().awaitIndexesOnline( timeout, unit );
                return null;
            }
        };
    }

    private DatabaseFunctions()
    {
        // no instances
    }
}
