/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.cursor.Cursor;
import org.neo4j.graphdb.Direction;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.schema.ConstraintValidationKernelException;
import org.neo4j.kernel.api.exceptions.schema.IndexBrokenKernelException;
import org.neo4j.kernel.api.index.IndexDescriptor;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.operations.EntityWriteOperations;
import org.neo4j.kernel.impl.util.register.NeoRegister;
import org.neo4j.register.Register;

public class GuardingStatementOperations implements
        EntityWriteOperations,
        EntityReadOperations
{
    private final EntityWriteOperations entityWriteDelegate;
    private final EntityReadOperations entityReadDelegate;
    private final Guard guard;

    public GuardingStatementOperations(
            EntityWriteOperations entityWriteDelegate,
            EntityReadOperations entityReadDelegate,
            Guard guard )
    {
        this.entityWriteDelegate = entityWriteDelegate;
        this.entityReadDelegate = entityReadDelegate;
        this.guard = guard;
    }

    @Override
    public long relationshipCreate( KernelStatement statement, int relationshipTypeId, long startNodeId, long endNodeId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.relationshipCreate( statement, relationshipTypeId, startNodeId, endNodeId );
    }

    @Override
    public long nodeCreate( KernelStatement statement )
    {
        guard.check();
        return entityWriteDelegate.nodeCreate( statement );
    }

    @Override
    public void nodeDelete( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        guard.check();
        entityWriteDelegate.nodeDelete( state, nodeId );
    }

    @Override
    public void relationshipDelete( KernelStatement state, long relationshipId ) throws EntityNotFoundException
    {
        guard.check();
        entityWriteDelegate.relationshipDelete( state, relationshipId );
    }

    @Override
    public boolean nodeAddLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException,
            ConstraintValidationKernelException
    {
        guard.check();
        return entityWriteDelegate.nodeAddLabel( state, nodeId, labelId );
    }

    @Override
    public boolean nodeRemoveLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        return entityWriteDelegate.nodeRemoveLabel( state, nodeId, labelId );
    }

    @Override
    public Property nodeSetProperty( KernelStatement state, long nodeId, DefinedProperty property )
            throws EntityNotFoundException, ConstraintValidationKernelException
    {
        guard.check();
        return entityWriteDelegate.nodeSetProperty( state, nodeId, property );
    }

    @Override
    public Property relationshipSetProperty( KernelStatement state, long relationshipId, DefinedProperty property )
            throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.relationshipSetProperty( state, relationshipId, property );
    }

    @Override
    public Property graphSetProperty( KernelStatement state, DefinedProperty property )
    {
        return entityWriteDelegate.graphSetProperty( state, property );
    }

    @Override
    public Property nodeRemoveProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.nodeRemoveProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipRemoveProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityWriteDelegate.relationshipRemoveProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphRemoveProperty( KernelStatement state, int propertyKeyId )
    {
        guard.check();
        return entityWriteDelegate.graphRemoveProperty( state, propertyKeyId );
    }

    @Override
    public boolean nodeExists( KernelStatement state, long nodeId )
    {
        guard.check();
        return entityReadDelegate.nodeExists( state, nodeId );
    }

    @Override
    public boolean relationshipExists( KernelStatement statement, long relId )
    {
        guard.check();
        return entityReadDelegate.relationshipExists( statement, relId );
    }

    @Override
    public PrimitiveLongIterator nodesGetForLabel( KernelStatement state, int labelId )
    {
        guard.check();
        return entityReadDelegate.nodesGetForLabel( state, labelId );
    }

    @Override
    public PrimitiveLongIterator nodesGetFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException
    {
        guard.check();
        return entityReadDelegate.nodesGetFromIndexLookup( state, index, value );
    }

    @Override
    public long nodeGetUniqueFromIndexLookup( KernelStatement state, IndexDescriptor index, Object value )
            throws IndexNotFoundKernelException, IndexBrokenKernelException
    {
        guard.check();
        return entityReadDelegate.nodeGetUniqueFromIndexLookup( state, index, value );
    }

    @Override
    public boolean nodeHasLabel( KernelStatement state, long nodeId, int labelId ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeHasLabel( state, nodeId, labelId );
    }

    @Override
    public PrimitiveIntIterator nodeGetLabels( KernelStatement state, long nodeId ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetLabels( state, nodeId );
    }

    @Override
    public Property nodeGetProperty( KernelStatement state, long nodeId, int propertyKeyId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetProperty( state, nodeId, propertyKeyId );
    }

    @Override
    public Property relationshipGetProperty( KernelStatement state, long relationshipId, int propertyKeyId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.relationshipGetProperty( state, relationshipId, propertyKeyId );
    }

    @Override
    public Property graphGetProperty( KernelStatement state, int propertyKeyId )
    {
        guard.check();
        return entityReadDelegate.graphGetProperty( state, propertyKeyId );
    }

    @Override
    public PrimitiveLongIterator nodeGetPropertyKeys( KernelStatement state, long nodeId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetPropertyKeys( state, nodeId );
    }

    @Override
    public Iterator<DefinedProperty> nodeGetAllProperties( KernelStatement state, long nodeId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetAllProperties( state, nodeId );
    }

    @Override
    public PrimitiveLongIterator relationshipGetPropertyKeys( KernelStatement state, long relationshipId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.relationshipGetPropertyKeys( state, relationshipId );
    }

    @Override
    public Iterator<DefinedProperty> relationshipGetAllProperties( KernelStatement state, long relationshipId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.relationshipGetAllProperties( state, relationshipId );
    }

    @Override
    public PrimitiveLongIterator graphGetPropertyKeys( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.graphGetPropertyKeys( state );
    }

    @Override
    public Iterator<DefinedProperty> graphGetAllProperties( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.graphGetAllProperties( state );
    }

    @Override
    public PrimitiveLongIterator nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction,
            int[] relTypes ) throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetRelationships( statement, nodeId, direction, relTypes );
    }

    @Override
    public PrimitiveLongIterator nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetRelationships( statement, nodeId, direction );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement, long nodeId, Direction direction, int relType )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetDegree( statement, nodeId, direction, relType );
    }

    @Override
    public int nodeGetDegree( KernelStatement statement, long nodeId, Direction direction )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetDegree( statement, nodeId, direction );
    }

    @Override
    public PrimitiveIntIterator nodeGetRelationshipTypes( KernelStatement statement, long nodeId )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetRelationshipTypes( statement, nodeId );
    }

    @Override
    public PrimitiveLongIterator nodesGetAll( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.nodesGetAll( state );
    }

    @Override
    public PrimitiveLongIterator relationshipsGetAll( KernelStatement state )
    {
        guard.check();
        return entityReadDelegate.relationshipsGetAll( state );
    }

    @Override
    public <EXCEPTION extends Exception> void relationshipVisit( KernelStatement statement, long relId,
            RelationshipVisitor<EXCEPTION> visitor )
            throws EntityNotFoundException, EXCEPTION
    {
        guard.check();
        entityReadDelegate.relationshipVisit( statement, relId, visitor );
    }

    @Override
    public Cursor expand( KernelStatement statement, Cursor inputCursor, NeoRegister.Node.In nodeId,
                          Register.Object.In<int[]> types, Register.Object.In<Direction> expandDirection,
                          NeoRegister.Relationship.Out relId, NeoRegister.RelType.Out relType,
                          Register.Object.Out<Direction> direction,
                          NeoRegister.Node.Out startNodeId, NeoRegister.Node.Out neighborNodeId )
    {
        guard.check();
        return entityReadDelegate.expand( statement, inputCursor, nodeId, types, expandDirection,
                relId, relType, direction, startNodeId, neighborNodeId );
    }

    @Override
    public Cursor nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction,
                                        final RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetRelationships( statement, nodeId, direction,
                                                        new GuardedRelationshipVisitor<>( guard, visitor ) );
    }

    @Override
    public Cursor nodeGetRelationships( KernelStatement statement, long nodeId, Direction direction, int[] types,
                                        RelationshipVisitor<? extends RuntimeException> visitor )
            throws EntityNotFoundException
    {
        guard.check();
        return entityReadDelegate.nodeGetRelationships( statement, nodeId, direction, types,
                                                        new GuardedRelationshipVisitor<>( guard, visitor ) );
    }

    private static class GuardedRelationshipVisitor<EX extends Exception> implements RelationshipVisitor<EX>
    {
        private final Guard guard;
        private final RelationshipVisitor<EX> visitor;

        public GuardedRelationshipVisitor( Guard guard, RelationshipVisitor<EX> visitor )
        {
            this.guard = guard;
            this.visitor = visitor;
        }

        @Override
        public void visit( long relId, int type, long startNode, long endNode ) throws EX
        {
            guard.check();
            visitor.visit( relId, type, startNode, endNode );
        }
    }
}
