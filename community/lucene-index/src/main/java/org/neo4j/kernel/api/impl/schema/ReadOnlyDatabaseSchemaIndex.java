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
package org.neo4j.kernel.api.impl.schema;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.index.ReadOnlyAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

/**
 * Read only schema index
 */
public class ReadOnlyDatabaseSchemaIndex extends ReadOnlyAbstractDatabaseIndex<LuceneSchemaIndex> implements SchemaIndex
{
    public ReadOnlyDatabaseSchemaIndex( PartitionedIndexStorage indexStorage, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, ReadOnlyIndexPartitionFactory readOnlyIndexPartitionFactory )
    {
        super( new LuceneSchemaIndex( indexStorage, descriptor, samplingConfig, readOnlyIndexPartitionFactory ) );
    }

    @Override
    public LuceneIndexWriter getIndexWriter()
    {
        throw new UnsupportedOperationException( "Can't get index writer for read only lucene index." );
    }

    @Override
    public IndexReader getIndexReader() throws IOException
    {
        return luceneIndex.getIndexReader();
    }

    @Override
    public SchemaIndexDescriptor getDescriptor()
    {
        return luceneIndex.getDescriptor();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void verifyUniqueness( PropertyAccessor accessor, int[] propertyKeyIds )
            throws IOException, IndexEntryConflictException
    {
        luceneIndex.verifyUniqueness( accessor, propertyKeyIds );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void verifyUniqueness( PropertyAccessor accessor, int[] propertyKeyIds, List<Value[]> updatedValueTuples )
            throws IOException, IndexEntryConflictException
    {
        luceneIndex.verifyUniqueness( accessor, propertyKeyIds, updatedValueTuples );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOnline() throws IOException
    {
        return luceneIndex.isOnline();
    }

    /**
     * Unsupported operation in read only index.
     */
    @Override
    public void markAsOnline()
    {
        throw new UnsupportedOperationException( "Can't mark read only index." );
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        luceneIndex.markAsFailed( failure );
    }
}
