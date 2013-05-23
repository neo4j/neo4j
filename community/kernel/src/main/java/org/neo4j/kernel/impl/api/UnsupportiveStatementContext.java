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

import java.util.Iterator;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.LabelNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.PropertyKeyIdNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyKeyNotFoundException;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.api.exceptions.schema.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.core.Token;

public enum UnsupportiveStatementContext implements StatementContext
{
    INSTANCE;

    public static StatementContext instance()
    {
        return INSTANCE;
    }

    @Override
    public void close()
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> nodesGetForLabel( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> nodesGetFromIndexLookup( IndexDescriptor index, Object value ) throws IndexNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public void nodeDelete( long nodeId )
    {
        throw unsupported();
    }

    @Override
    public long labelGetOrCreateForName( String label )
    {
        throw unsupported();
    }

    @Override
    public long labelGetForName( String label ) throws LabelNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public String labelGetName( long labelId ) throws LabelNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public boolean nodeAddLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public boolean nodeHasLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> nodeGetLabels( long nodeId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public boolean nodeRemoveLabel( long nodeId, long labelId ) throws EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<Token> labelsGetAllTokens()
    {
        throw unsupported();
    }

    @Override
    public long propertyKeyGetOrCreateForName( String propertyKey )
    {
        throw unsupported();
    }

    @Override
    public long propertyKeyGetForName( String propertyKey ) throws PropertyKeyNotFoundException
    {
        throw unsupported();
    }

    @Override
    public String propertyKeyGetName( long propertyId ) throws PropertyKeyIdNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Object nodeGetPropertyValue( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
                                                                              PropertyNotFoundException,
                                                                              EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public boolean nodeHasProperty( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
                                                                          EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public void nodeSetPropertyValue( long nodeId, long propertyId,
                                      Object value ) throws PropertyKeyIdNotFoundException, EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Object nodeRemoveProperty( long nodeId, long propertyId ) throws PropertyKeyIdNotFoundException,
                                                                            EntityNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> nodeGetPropertyKeys( long nodeId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<Long> relationshipGetPropertyKeys( long relationshipId )
    {
        throw unsupported();
    }

    @Override
    public IndexDescriptor indexCreate( long labelId, long propertyKey )
    {
        throw unsupported();
    }

    @Override
    public IndexDescriptor uniqueIndexCreate( long labelId, long propertyKey )
    {
        throw unsupported();
    }

    @Override
    public IndexDescriptor indexesGetForLabelAndPropertyKey( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetForLabel( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> indexesGetAll()
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetForLabel( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<IndexDescriptor> uniqueIndexesGetAll()
    {
        throw unsupported();
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        throw unsupported();
    }

    @Override
    public void indexDrop( IndexDescriptor descriptor )
    {
        throw unsupported();
    }

    @Override
    public void uniqueIndexDrop( IndexDescriptor descriptor )
    {
        throw unsupported();
    }

    @Override
    public <K, V> V schemaStateGetOrCreate( K key, Function<K, V> creator )
    {
        throw unsupported();
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        throw unsupported();
    }

    @Override
    public UniquenessConstraint uniquenessConstraintCreate( long labelId, long propertyKeyId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabelAndPropertyKey( long labelId, long propertyKeyId )
    {
        throw unsupported();
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetForLabel( long labelId )
    {
        throw unsupported();
    }

    @Override
    public Long indexGetOwningUniquenessConstraintId( IndexDescriptor index )
    {
        throw unsupported();
    }

    @Override
    public long indexGetCommittedId( IndexDescriptor index ) throws SchemaRuleNotFoundException
    {
        throw unsupported();
    }

    @Override
    public Iterator<UniquenessConstraint> constraintsGetAll()
    {
        throw unsupported();
    }

    @Override
    public void constraintDrop( UniquenessConstraint constraint )
    {
        throw unsupported();
    }

    private UnsupportedOperationException unsupported()
    {
        return new UnsupportedOperationException( "This operation is not implemented." );
    }
}
