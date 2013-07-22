/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.graphdb;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static org.neo4j.graphdb.DynamicLabel.label;

/**
 * Test convenience: all the methods on GraphDatabaseService, callable using generic interface
 */
class GraphDatabaseServiceMethods
{
    static final Set<GraphDatabaseServiceMethod> allGraphDatabaseServiceMethods()
    {
        Set<GraphDatabaseServiceMethod> gdsMethods = new HashSet<>();

        // wrap them in nice pretty printing
        for ( final Field field : GraphDatabaseServiceMethods.class.getDeclaredFields() )
        {
            gdsMethods.add( new GraphDatabaseServiceMethod()
            {
                @Override
                public void call( GraphDatabaseService graphDatabaseService )
                {
                    try
                    {
                        GraphDatabaseServiceMethod graphDatabaseServiceMethod = (GraphDatabaseServiceMethod) field
                                .get( null );

                        graphDatabaseServiceMethod.call( graphDatabaseService );
                    }
                    catch ( IllegalAccessException e )
                    {
                        throw new UnsupportedOperationException( "TODO", e );
                    }
                }

                @Override
                public String toString()
                {
                    return field.getName();
                }
            } );
        }

        return gdsMethods;
    }

    static final GraphDatabaseServiceMethod createNode = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.createNode();
        }
    };

    static final GraphDatabaseServiceMethod createNodeWithLabels = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.createNode( label( "FOO" ) );
        }
    };

    static final GraphDatabaseServiceMethod getNodeById = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getNodeById( 42 );
        }
    };

    static final GraphDatabaseServiceMethod getRelationshipById = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getRelationshipById( 87 );
        }
    };

    static final GraphDatabaseServiceMethod getReferenceNode = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getReferenceNode();
        }
    };

    static final GraphDatabaseServiceMethod getAllNodes = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            for ( Node node : graphDatabaseService.getAllNodes() )
            {

            }
        }
    };

    static final GraphDatabaseServiceMethod findNodesByLabelAndProperty = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            for ( Node node : graphDatabaseService.findNodesByLabelAndProperty( label( "bar" ), "baz", 23 ) )
            {

            }
        }
    };

    static final GraphDatabaseServiceMethod getRelationshipTypes = new GraphDatabaseServiceMethod()
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getRelationshipTypes();
        }
    };
}
