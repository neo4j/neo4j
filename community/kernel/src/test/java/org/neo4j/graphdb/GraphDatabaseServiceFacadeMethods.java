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
package org.neo4j.graphdb;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;

import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.IteratorUtil.loop;

/**
 * Test convenience: all the methods on GraphDatabaseService, callable using generic interface
 */
public class GraphDatabaseServiceFacadeMethods
{
    static final FacadeMethod<GraphDatabaseService> CREATE_NODE =
        new FacadeMethod<GraphDatabaseService>( "Node createNode()" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.createNode();
        }
    };

    static final FacadeMethod<GraphDatabaseService> CREATE_NODE_WITH_LABELS =
        new FacadeMethod<GraphDatabaseService>( "Node createNode( Label... labels )" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.createNode( label( "FOO" ) );
        }
    };

    static final FacadeMethod<GraphDatabaseService> GET_NODE_BY_ID =
        new FacadeMethod<GraphDatabaseService>( "Node getNodeById( long id )" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getNodeById( 42 );
        }
    };

    static final FacadeMethod<GraphDatabaseService> GET_RELATIONSHIP_BY_ID =
        new FacadeMethod<GraphDatabaseService>( "Relationship getRelationshipById( long id )" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getRelationshipById( 87 );
        }
    };

    static final FacadeMethod<GraphDatabaseService> GET_ALL_NODES =
            new FacadeMethod<GraphDatabaseService>( "Iterable<Node> getAllNodes()" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            for ( Node node : graphDatabaseService.getAllNodes() )
            {

            }
        }
    };

    static final FacadeMethod<GraphDatabaseService> FIND_NODES_BY_LABEL_AND_PROPERTY =
        new FacadeMethod<GraphDatabaseService>(
                "ResourceIterator<Node> findNodes( Label label, String key, Object value )" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            for ( Node node : graphDatabaseService.findNodesByLabelAndProperty( label( "bar" ), "baz", 23 ) )
            {

            }
        }
    };

    static final FacadeMethod<GraphDatabaseService> FIND_NODES_BY_LABEL_AND_PROPERTY_DEPRECATED =
        new FacadeMethod<GraphDatabaseService>(
                "ResourceIterator<Node> findNodeByLabelAndProperty( Label label, String key, Object value )" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            for ( Node node : loop( graphDatabaseService.findNodes( label( "bar" ), "baz", 23 ) ) )
            {

            }
        }
    };

    static final FacadeMethod<GraphDatabaseService> FIND_NODES_BY_LABEL =
            new FacadeMethod<GraphDatabaseService>(
                    "ResourceIterator<Node> findNodes( Label label )" )
            {
                @Override
                public void call( GraphDatabaseService graphDatabaseService )
                {
                    for ( Node node : loop( graphDatabaseService.findNodes( label( "bar" ) ) ) )
                    {

                    }
                }
            };

    static final FacadeMethod<GraphDatabaseService> GET_RELATIONSHIP_TYPES =
            new FacadeMethod<GraphDatabaseService>( "Iterable<RelationshipType> getRelationshipTypes()" )
    {
        @Override
        public void call( GraphDatabaseService graphDatabaseService )
        {
            graphDatabaseService.getRelationshipTypes();
        }
    };

    static final FacadeMethod<GraphDatabaseService> SCHEMA =
            new FacadeMethod<GraphDatabaseService>( "Schema schema()" )
            {
                @Override
                public void call( GraphDatabaseService graphDatabaseService )
                {
                    graphDatabaseService.schema();
                }
            };

    static final Iterable<FacadeMethod<GraphDatabaseService>> ALL_NON_TRANSACTIONAL_GRAPH_DATABASE_METHODS =
        unmodifiableCollection( asList(
            CREATE_NODE,
            CREATE_NODE_WITH_LABELS,
            GET_NODE_BY_ID,
            GET_RELATIONSHIP_BY_ID,
            GET_ALL_NODES,
            FIND_NODES_BY_LABEL_AND_PROPERTY,
            FIND_NODES_BY_LABEL_AND_PROPERTY_DEPRECATED,
            FIND_NODES_BY_LABEL,
            GET_RELATIONSHIP_TYPES,
            SCHEMA
            // TODO: INDEX
        ) );


}
