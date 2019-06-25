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
package org.neo4j.kernel.impl.storageengine.impl.recordstorage;

import java.util.Iterator;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotFoundKernelException;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.internal.kernel.api.schema.constraints.ConstraintDescriptor;
import org.neo4j.kernel.impl.api.store.SchemaCache;
import org.neo4j.storageengine.api.StorageSchemaReader;
import org.neo4j.storageengine.api.schema.CapableIndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.PopulationProgress;

class StorageSchemaReaderSnapshot implements StorageSchemaReader
{
    private final SchemaCache schema;
    private final RecordStorageReader reader;

    StorageSchemaReaderSnapshot( SchemaCache schema, RecordStorageReader reader )
    {
        this.schema = schema;
        this.reader = reader;
    }

    @Override
    public CapableIndexDescriptor indexGetForSchema( SchemaDescriptor descriptor )
    {
        return schema.indexDescriptor( descriptor );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetForLabel( int labelId )
    {
        return schema.indexDescriptorsForLabel( labelId );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetForRelationshipType( int relationshipType )
    {
        return schema.indexDescriptorsForRelationshipType( relationshipType );
    }

    @Override
    public Iterator<CapableIndexDescriptor> indexesGetAll()
    {
        return schema.indexDescriptors().iterator();
    }

    @Override
    public InternalIndexState indexGetState( IndexDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return reader.indexGetState( descriptor );
    }

    @Override
    public PopulationProgress indexGetPopulationProgress( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return reader.indexGetPopulationProgress( descriptor );
    }

    @Override
    public String indexGetFailure( SchemaDescriptor descriptor ) throws IndexNotFoundKernelException
    {
        return reader.indexGetFailure( descriptor );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForLabel( int labelId )
    {
        return schema.constraintsForLabel( labelId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetForRelationshipType( int typeId )
    {
        return schema.constraintsForRelationshipType( typeId );
    }

    @Override
    public Iterator<ConstraintDescriptor> constraintsGetAll()
    {
        return schema.constraints();
    }
}
