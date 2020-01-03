/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.impl.newapi;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.IndexReference;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.SchemaReadCore;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.PopulationProgress;

class SchemaReadCoreSnapshot implements SchemaReadCore
{
    private final StorageSchemaReader snapshot;
    private final KernelTransactionImplementation ktx;
    private final AllStoreHolder stores;

    SchemaReadCoreSnapshot( StorageSchemaReader snapshot, KernelTransactionImplementation ktx, AllStoreHolder stores )
    {
        this.snapshot = snapshot;
        this.ktx = ktx;
        this.stores = stores;
    }

    @Override
    public IndexReference index( SchemaDescriptor schema )
    {
        ktx.assertOpen();
        return stores.indexGetForSchema( snapshot, schema );
    }

    @Override
    public Iterator<IndexReference> indexesGetForLabel( int labelId )
    {
        ktx.assertOpen();
        return stores.indexesGetForLabel( snapshot, labelId );
    }

    @Override
    public Iterator<IndexReference> indexesGetForRelationshipType( int relationshipType )
    {
        ktx.assertOpen();
        return stores.indexesGetForRelationshipType( snapshot, relationshipType );
    }

    @Override
    public Iterator<IndexReference> indexesGetAll()
    {
        ktx.assertOpen();
        //noinspection unchecked
        return (Iterator) stores.indexesGetAll( snapshot );
    }

    @Override
    public InternalIndexState indexGetState( IndexReference index ) throws IndexNotFoundKernelException
    {
        AllStoreHolder.assertValidIndex( index );
        ktx.assertOpen();
        return stores.indexGetState( snapshot, (IndexDescriptor) index );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( IndexReference index ) throws IndexNotFoundKernelException
    {
        AllStoreHolder.assertValidIndex( index );
        ktx.assertOpen();
        return stores.indexGetPopulationProgress( snapshot, index );
    }

    @Override
    public String indexGetFailure( IndexReference index ) throws IndexNotFoundKernelException
    {
        AllStoreHolder.assertValidIndex( index );
        return snapshot.indexGetFailure( index.schema() );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        ktx.assertOpen();
        return stores.constraintsGetForLabel( snapshot, labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        ktx.assertOpen();
        return stores.constraintsGetForRelationshipType( snapshot, typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        ktx.assertOpen();
        return stores.constraintsGetAll( snapshot );
    }
}
