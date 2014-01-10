/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.RelationshipTypeIdNotFoundKernelException;
import org.neo4j.kernel.impl.api.operations.LegacyKernelOperations;
import org.neo4j.kernel.impl.core.NodeImpl;
import org.neo4j.kernel.impl.core.NodeManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.util.PrimitiveLongIterator;

public class DefaultLegacyKernelOperations implements LegacyKernelOperations
{
    private final NodeManager nodeManager;

    public DefaultLegacyKernelOperations( NodeManager nodeManager )
    {
        this.nodeManager = nodeManager;
    }

    @Override
    public long nodeCreate( Statement state )
    {
        return nodeManager.createNode().getId();
    }

    @Override
    public long relationshipCreate( Statement state, long relationshipTypeId, long startNodeId, long endNodeId )
            throws RelationshipTypeIdNotFoundKernelException, EntityNotFoundException
    {
        NodeImpl startNode;
        try
        {
            startNode = nodeManager.getNodeForProxy( startNodeId, LockType.WRITE );
        }
        catch ( NotFoundException e )
        {
            throw new EntityNotFoundException( EntityType.NODE, startNodeId, e );
        }
        try
        {
            return nodeManager.createRelationship( nodeManager.newNodeProxyById( startNodeId ), startNode,
                                                   nodeManager.newNodeProxyById( endNodeId ), relationshipTypeId )
                              .getId();
        }
        catch ( NotFoundException e )
        {
            throw new EntityNotFoundException( EntityType.NODE, endNodeId, e );
        }
    }

    @Override
    public PrimitiveLongIterator relationshipsGetFromNode( KernelStatement statement, long nodeId, Direction direction, int[] relTypes )
    {
        return relsToPrimitiveIterator( nodeManager.getNodeForProxy( nodeId, null )
                .getRelationships( nodeManager, direction, relIdsToGDSRels(statement, relTypes) ) );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetFromNode( KernelStatement statement, long nodeId, Direction direction )
    {
        return relsToPrimitiveIterator( nodeManager.getNodeForProxy( nodeId, null )
                .getRelationships( nodeManager, direction ) );
    }

    private PrimitiveLongIterator relsToPrimitiveIterator( Iterable<Relationship> relationships )
    {
        final Iterator<Relationship> rels = relationships.iterator();
        return new PrimitiveLongIterator()
        {
            @Override
            public boolean hasNext()
            {
                return rels.hasNext();
            }

            @Override
            public long next()
            {
                return rels.next().getId();
            }
        };
    }

    private RelationshipType[] relIdsToGDSRels( KernelStatement statement, int[] relTypes )
    {
        RelationshipType[] out = new RelationshipType[relTypes.length];
        for(int i=0;i<relTypes.length;i++)
        {
            out[i] = relIdToGDSType( statement, relTypes[i] );
        }
        return out;
    }

    private RelationshipType relIdToGDSType( KernelStatement statement, int relType )
    {
        try
        {
            return DynamicRelationshipType.withName(statement.readOperations().relationshipTypeGetName( relType ));
        }
        catch ( RelationshipTypeIdNotFoundKernelException e )
        {
            throw new ThisShouldNotHappenError( "Jake", "Mapping from a known reltype id to a reltype name " +
                    "should not fail." );
        }
    }
}
