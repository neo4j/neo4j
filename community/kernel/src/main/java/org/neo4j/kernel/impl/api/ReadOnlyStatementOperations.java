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

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.DropIndexFailureException;
import org.neo4j.kernel.api.exceptions.schema.SchemaKernelException;
import org.neo4j.kernel.api.operations.EntityWriteOperations;
import org.neo4j.kernel.api.operations.KeyReadOperations;
import org.neo4j.kernel.api.operations.KeyWriteOperations;
import org.neo4j.kernel.api.operations.SchemaStateOperations;
import org.neo4j.kernel.api.operations.SchemaWriteOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.api.properties.SafeProperty;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.ReadOnlyDbException;

public class ReadOnlyStatementOperations implements
    KeyWriteOperations,
    EntityWriteOperations,
    SchemaWriteOperations,
    SchemaStateOperations
{
    private final KeyReadOperations keyReadOperations;
    private final SchemaStateOperations schemaStateDelegate;

    public ReadOnlyStatementOperations( KeyReadOperations keyReadOperations, SchemaStateOperations schemaStateDelegate )
    {
        this.keyReadOperations = keyReadOperations;
        this.schemaStateDelegate = schemaStateDelegate;
    }

    @Override
    public long labelGetOrCreateForName( StatementState state, String labelName ) throws SchemaKernelException
    {
        // Just get, returning NO_SUCH_LABEL if there is none.
        // Lookup using that constant will yield nothing, no node has it,
        // and since this is read-only, the user will not be able to set it.
        return keyReadOperations.labelGetForName( state, labelName );
    }

    @Override
    public long propertyKeyGetOrCreateForName( StatementState state, String propertyKeyName ) throws SchemaKernelException
    {
        // Just get, returning NO_SUCH_PROPERTY_KEY if there is none,
        // Lookup using that constant will yield nothing, no node has it,
        // and since this is read-only, the user will not be able to set it.
        return keyReadOperations.propertyKeyGetForName( state, propertyKeyName );
    }

    @Override
    public void nodeDelete( StatementState state, long nodeId )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public void relationshipDelete( StatementState state, long relationshipId )
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public boolean nodeAddLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public boolean nodeRemoveLabel( StatementState state, long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Property nodeSetProperty( StatementState state, long nodeId, SafeProperty property ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Property relationshipSetProperty( StatementState state, long relationshipId, SafeProperty property )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Property graphSetProperty( StatementState state, SafeProperty property ) throws PropertyKeyIdNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Property nodeRemoveProperty( StatementState state, long nodeId, long propertyKeyId ) throws PropertyKeyIdNotFoundException,
            EntityNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Property relationshipRemoveProperty( StatementState state, long relationshipId, long propertyKeyId )
            throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public Property graphRemoveProperty( StatementState state, long propertyKeyId ) throws PropertyKeyIdNotFoundException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public IndexDescriptor indexCreate( StatementState state, long labelId, long propertyKeyId ) throws SchemaKernelException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public void indexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public void uniqueIndexDrop( StatementState state, IndexDescriptor descriptor ) throws DropIndexFailureException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( StatementState state, long labelId, long propertyKeyId )
            throws SchemaKernelException
    {
        throw new ReadOnlyDbException();
    }

    @Override
    public void constraintDrop( StatementState state, UniquenessConstraint constraint )
    {
        throw new ReadOnlyDbException();
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
