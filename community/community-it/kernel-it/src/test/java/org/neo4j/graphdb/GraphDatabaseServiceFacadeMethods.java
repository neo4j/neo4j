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
package org.neo4j.graphdb;

import java.util.function.Consumer;

import static org.neo4j.graphdb.FacadeMethod.consume;
import static org.neo4j.graphdb.Label.label;

/**
 * Test convenience: all the methods on GraphDatabaseService, callable using generic interface
 */
public enum GraphDatabaseServiceFacadeMethods implements Consumer<GraphDatabaseService>
{
    GET_NODE_BY_ID( new FacadeMethod<>( "Node getNodeById( long id )", gds -> gds.getNodeById( 42 ) ) ),
    GET_RELATIONSHIP_BY_ID( new FacadeMethod<>( "Relationship getRelationshipById( long id )", gds -> gds.getRelationshipById( 42 ) ) ),
    GET_ALL_NODES( new FacadeMethod<>( "Iterable<Node> getAllNodes()", gds -> consume( gds.getAllNodes() ) ) ),
    FIND_NODES_BY_LABEL_AND_PROPERTY_DEPRECATED(
            new FacadeMethod<>( "ResourceIterator<Node> findNodeByLabelAndProperty( Label label, String key, Object value )",
                    gds -> consume( gds.findNodes( label( "bar" ), "baz", 23 ) ) ) ),
    FIND_NODES_BY_LABEL( new FacadeMethod<>( "ResourceIterator<Node> findNodes( Label label )", gds -> consume( gds.findNodes( label( "bar" ) ) ) ) ),
    SCHEMA( new FacadeMethod<>( "Schema schema()", GraphDatabaseService::schema ) );

    private final FacadeMethod<GraphDatabaseService> facadeMethod;

    GraphDatabaseServiceFacadeMethods( FacadeMethod<GraphDatabaseService> facadeMethod )
    {
        this.facadeMethod = facadeMethod;
    }

    @Override
    public void accept( GraphDatabaseService graphDatabaseService )
    {
        facadeMethod.accept( graphDatabaseService );
    }

    @Override
    public String toString()
    {
        return facadeMethod.toString();
    }
}
