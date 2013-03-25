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
import org.neo4j.kernel.api.ConstraintViolationKernelException;
import org.neo4j.kernel.api.SchemaRuleNotFoundException;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;

public class DelegatingSchemaOperations implements SchemaOperations
{
    protected final SchemaOperations delegate;

    public DelegatingSchemaOperations( SchemaOperations delegate )
    {
        this.delegate = delegate;
    }

    @Override
    public IndexRule addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        return delegate.addIndexRule( labelId, propertyKey );
    }

    @Override
    public void dropIndexRule( IndexRule indexRule ) throws ConstraintViolationKernelException
    {
        delegate.dropIndexRule( indexRule );
    }

    @Override
    public IndexDescriptor getIndexDescriptor( long indexId ) throws IndexNotFoundKernelException
    {
        return delegate.getIndexDescriptor( indexId );
    }

    @Override
    public IndexRule getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        return delegate.getIndexRule( labelId, propertyKey );
    }

    @Override
    public Iterator<IndexRule> getIndexRules()
    {
        return delegate.getIndexRules();
    }

    @Override
    public Iterator<IndexRule> getIndexRules( long labelId )
    {
        return delegate.getIndexRules( labelId );
    }

    @Override
    public InternalIndexState getIndexState( IndexRule indexRule ) throws IndexNotFoundKernelException
    {
        return delegate.getIndexState( indexRule );
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        return delegate.getOrCreateFromSchemaState( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        return delegate.schemaStateContains( key );
    }
}
