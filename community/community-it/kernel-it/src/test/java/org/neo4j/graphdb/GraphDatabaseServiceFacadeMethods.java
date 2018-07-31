/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableCollection;
import static org.neo4j.graphdb.FacadeMethod.consume;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterators.loop;

/**
 * Test convenience: all the methods on GraphDatabaseService, callable using generic interface
 */
public class GraphDatabaseServiceFacadeMethods
{
    static final FacadeMethod<GraphDatabaseService> CREATE_NODE = new FacadeMethod<>( "Node createNode()", GraphDatabaseService::createNode );
    static final FacadeMethod<GraphDatabaseService> CREATE_NODE_WITH_LABELS = new FacadeMethod<>( "Node createNode( Label... labels )", gds -> gds.createNode( label( "FOO" ) ) );
    static final FacadeMethod<GraphDatabaseService> GET_NODE_BY_ID = new FacadeMethod<>( "Node getNodeById( long id )", gds -> gds.getNodeById( 42 ) );
    static final FacadeMethod<GraphDatabaseService> GET_RELATIONSHIP_BY_ID = new FacadeMethod<>( "Relationship getRelationshipById( long id )", gds -> gds.getRelationshipById( 42 ) );
    static final FacadeMethod<GraphDatabaseService> GET_ALL_NODES = new FacadeMethod<>( "Iterable<Node> getAllNodes()", gds -> consume( gds.getAllNodes() ) );
    static final FacadeMethod<GraphDatabaseService> FIND_NODES_BY_LABEL_AND_PROPERTY_DEPRECATED = new FacadeMethod<>(
                "ResourceIterator<Node> findNodeByLabelAndProperty( Label label, String key, Object value )", gds -> consume( gds.findNodes( label( "bar" ), "baz", 23 ) ) );
    static final FacadeMethod<GraphDatabaseService> FIND_NODES_BY_LABEL = new FacadeMethod<>(
                    "ResourceIterator<Node> findNodes( Label label )", gds -> consume( gds.findNodes( label( "bar" ) ) ) );
    static final FacadeMethod<GraphDatabaseService> GET_ALL_LABELS = new FacadeMethod<>( "Iterable<Label> getAllLabels()", GraphDatabaseService::getAllLabels );
    static final FacadeMethod<GraphDatabaseService> GET_ALL_LABELS_IN_USE = new FacadeMethod<>( "Iterable<Label> getAllLabelsInUse()", GraphDatabaseService::getAllLabelsInUse );
    static final FacadeMethod<GraphDatabaseService> GET_ALL_RELATIONSHIP_TYPES = new FacadeMethod<>( "Iterable<RelationshipType> getAllRelationshipTypes()", GraphDatabaseService::getAllRelationshipTypes );
    static final FacadeMethod<GraphDatabaseService> GET_ALL_RELATIONSHIP_TYPES_IN_USE = new FacadeMethod<>( "Iterable<RelationshipType> getAllRelationshipTypesInUse()", GraphDatabaseService::getAllRelationshipTypesInUse );
    static final FacadeMethod<GraphDatabaseService> GET_ALL_PROPERTY_KEYS = new FacadeMethod<>( "Iterable<String> getAllPropertyKeys()", GraphDatabaseService::getAllPropertyKeys );
    static final FacadeMethod<GraphDatabaseService> SCHEMA = new FacadeMethod<>( "Schema schema()", GraphDatabaseService::schema );
    static final FacadeMethod<GraphDatabaseService> INDEX = new FacadeMethod<>( "IndexManager index()", GraphDatabaseService::index );

    static final Iterable<FacadeMethod<GraphDatabaseService>> ALL_NON_TRANSACTIONAL_GRAPH_DATABASE_METHODS =
        unmodifiableCollection( asList(
                CREATE_NODE,
                CREATE_NODE_WITH_LABELS,
                GET_NODE_BY_ID,
                GET_RELATIONSHIP_BY_ID,
                GET_ALL_NODES,
                FIND_NODES_BY_LABEL_AND_PROPERTY_DEPRECATED,
                FIND_NODES_BY_LABEL,
                GET_ALL_LABELS,
                GET_ALL_LABELS_IN_USE,
                GET_ALL_RELATIONSHIP_TYPES,
                GET_ALL_RELATIONSHIP_TYPES_IN_USE,
                GET_ALL_PROPERTY_KEYS,
                SCHEMA,
                INDEX
        ) );

    private GraphDatabaseServiceFacadeMethods()
    {
    }
}
