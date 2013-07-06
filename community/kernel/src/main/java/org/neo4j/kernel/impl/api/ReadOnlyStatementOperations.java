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
package org.neo4j.kernel.impl.api;

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class ReadOnlyStatementOperations implements
    KeyWriteOperations,
    EntityWriteOperations,
    SchemaWriteOperations,
    SchemaStateOperations
{
    private final SchemaStateOperations schemaStateDelegate;

    public ReadOnlyStatementOperations(
            SchemaStateOperations schemaStateDelegate )
    {
        this.schemaStateDelegate = schemaStateDelegate;
    }

    public NotInTransactionException notInTransaction()
    {
        return new NotInTransactionException(
                "You have to be in a transaction context to perform write operations." );
    }

    @Override
    public long labelGetOrCreateForName( StatementState state, String labelName ) throws SchemaKernelException
    {
        throw notInTransaction();
    }

    @Override
    public long propertyKeyGetOrCreateForName( StatementState state, String propertyKeyName ) throws SchemaKernelException
    {
        throw notInTransaction();
    }

    @Override
    public void nodeDelete( StatementState state, long nodeId )
    {
        throw notInTransaction();
    }

    @Override
    public void relationshipDelete( StatementState state, long relationshipId )
    {
        throw notInTransaction();
    }

    @Override
    public boolean nodeAddLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public boolean nodeRemoveLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public Property nodeSetProperty( StatementState state, long nodeId, Property property ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public Property relationshipSetProperty( StatementState state, long relationshipId, Property property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public Property graphSetProperty( StatementState state, Property property ) throws PropertyKeyIdNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public Property nodeRemoveProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public Property relationshipRemoveProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public Property graphRemoveProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        throw notInTransaction();
    }

    @Override
    public IndexDescriptor indexCreate( StatementState state, long labelId, long propertyKeyId ) throws SchemaKernelException
    {
        throw notInTransaction();
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( StatementState state, long labelId, long propertyKey ) throws SchemaKernelException
    {
        throw notInTransaction();
    }

    @Override
    public void indexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        throw notInTransaction();
    }

    @Override
    public void uniqueIndexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        throw notInTransaction();
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( StatementState state, long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        throw notInTransaction();
    }

    @Override
    public void constraintDrop( StatementState state, UniquenessConstraint constraint )
    {
        throw notInTransaction();
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( StatementState state, K key, Function<K, V> creator )
    {
        return schemaStateDelegate.schemaStateGetOrCreate( state, key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( StatementState state, K key )
    {
        return schemaStateDelegate.schemaStateContains( state, key );
    }
}
