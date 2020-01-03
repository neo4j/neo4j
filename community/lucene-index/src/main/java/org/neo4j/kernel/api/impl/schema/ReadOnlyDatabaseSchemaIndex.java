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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.ReadOnlyAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.storageengine.api.NodePropertyAccessor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexDescriptor;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

/**
 * Read only schema index
 */
public class ReadOnlyDatabaseSchemaIndex extends ReadOnlyAbstractDatabaseIndex<LuceneSchemaIndex,IndexReader> implements SchemaIndex
{
    public ReadOnlyDatabaseSchemaIndex( PartitionedIndexStorage indexStorage, IndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, ReadOnlyIndexPartitionFactory readOnlyIndexPartitionFactory )
    {
        super( new LuceneSchemaIndex( indexStorage, descriptor, samplingConfig, readOnlyIndexPartitionFactory ) );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void verifyUniqueness( NodePropertyAccessor accessor, int[] propertyKeyIds )
            throws IOException, IndexEntryConflictException
    {
        luceneIndex.verifyUniqueness( accessor, propertyKeyIds );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void verifyUniqueness( NodePropertyAccessor accessor, int[] propertyKeyIds, List<Value[]> updatedValueTuples )
            throws IOException, IndexEntryConflictException
    {
        luceneIndex.verifyUniqueness( accessor, propertyKeyIds, updatedValueTuples );
    }
}
