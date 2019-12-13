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
package org.neo4j.server.rest.domain;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.security.AnonymousContext;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static org.neo4j.kernel.api.KernelTransaction.Type.IMPLICIT;

public class GraphDbHelper
{
    private final GraphDatabaseAPI databaseService;

    public GraphDbHelper( GraphDatabaseService databaseService )
    {
        this.databaseService = (GraphDatabaseAPI) databaseService;
    }

    public long createNode( Label... labels )
    {
        try ( Transaction tx = databaseService.beginTransaction( IMPLICIT, AnonymousContext.writeToken() ) )
        {
            Node node = tx.createNode( labels );
            tx.commit();
            return node.getId();
        }
    }

    public long createNode( Map<String, Object> properties, Label... labels )
    {
        try ( Transaction tx = databaseService.beginTransaction( IMPLICIT, AnonymousContext.writeToken() ) )
        {
            Node node = tx.createNode( labels );
            for ( Map.Entry<String, Object> entry : properties.entrySet() )
            {
                node.setProperty( entry.getKey(), entry.getValue() );
            }
            tx.commit();
            return node.getId();
        }
    }

    public long createRelationship( String type, long startNodeId, long endNodeId )
    {
        try ( Transaction tx = databaseService.beginTransaction( IMPLICIT, AnonymousContext.writeToken() ) )
        {
            Node startNode = tx.getNodeById( startNodeId );
            Node endNode = tx.getNodeById( endNodeId );
            Relationship relationship = startNode.createRelationshipTo( endNode, RelationshipType.withName( type ) );
            tx.commit();
            return relationship.getId();
        }
    }
}
