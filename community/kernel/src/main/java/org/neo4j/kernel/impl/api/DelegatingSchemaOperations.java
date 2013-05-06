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
import org.neo4j.kernel.api.constraints.UniquenessConstraint;
import org.neo4j.kernel.api.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.api.operations.SchemaOperations;
import org.neo4j.kernel.impl.api.index.IndexDescriptor;

public class DelegatingSchemaOperations implements SchemaOperations
{
    private final SchemaOperations schemaOperations;

    public DelegatingSchemaOperations( SchemaOperations schemaOperations )
    {
        this.schemaOperations = schemaOperations;
    }

    @Override
    public IndexDescriptor getIndexRule( long labelId, long propertyKey ) throws SchemaRuleNotFoundException
    {
        return schemaOperations.getIndexRule( labelId, propertyKey );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexRules( long labelId )
    {
        return schemaOperations.getIndexRules( labelId );
    }

    @Override
    public Iterator<IndexDescriptor> getIndexRules()
    {
        return schemaOperations.getIndexRules();
    }

    @Override
    public InternalIndexState getIndexState( IndexDescriptor indexRule ) throws IndexNotFoundKernelException
    {
        return schemaOperations.getIndexState( indexRule );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId, long propertyKeyId )
    {
        return schemaOperations.getConstraints( labelId, propertyKeyId );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints( long labelId )
    {
        return schemaOperations.getConstraints( labelId );
    }

    @Override
    public Iterator<UniquenessConstraint> getConstraints()
    {
        return schemaOperations.getConstraints();
    }

    @Override
    public IndexDescriptor addIndexRule( long labelId, long propertyKey ) throws ConstraintViolationKernelException
    {
        return schemaOperations.addIndexRule( labelId, propertyKey );
    }

    @Override
    public void dropIndexRule( IndexDescriptor indexRule ) throws ConstraintViolationKernelException
    {
        schemaOperations.dropIndexRule( indexRule );
    }

    @Override
    public UniquenessConstraint addUniquenessConstraint( long labelId,
                                                         long propertyKeyId ) throws ConstraintViolationKernelException
    {
        return schemaOperations.addUniquenessConstraint( labelId, propertyKeyId );
    }

    @Override
    public void dropConstraint( UniquenessConstraint constraint )
    {
        schemaOperations.dropConstraint( constraint );
    }

    @Override
    public <K, V> V getOrCreateFromSchemaState( K key, Function<K, V> creator )
    {
        return schemaOperations.getOrCreateFromSchemaState( key, creator );
    }

    @Override
    public <K> boolean schemaStateContains( K key )
    {
        return schemaOperations.schemaStateContains( key );
    }
}
