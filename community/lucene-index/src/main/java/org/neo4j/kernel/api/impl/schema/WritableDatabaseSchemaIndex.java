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
import org.neo4j.kernel.api.impl.index.WritableAbstractDatabaseIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.PropertyAccessor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.storageengine.api.schema.IndexReader;
import org.neo4j.values.storable.Value;

/**
 * Writable schema index
 */
public class WritableDatabaseSchemaIndex extends WritableAbstractDatabaseIndex<LuceneSchemaIndex> implements SchemaIndex
{

    public WritableDatabaseSchemaIndex( PartitionedIndexStorage storage, SchemaIndexDescriptor descriptor,
            IndexSamplingConfig samplingConfig, WritableIndexPartitionFactory writableIndexPartitionFactory )
    {
        super( new LuceneSchemaIndex( storage, descriptor, samplingConfig, writableIndexPartitionFactory ) );
    }

    @Override
    public LuceneIndexWriter getIndexWriter()
    {
        return luceneIndex.getIndexWriter( this );
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
     * {@inheritDoc}
     */
    @Override
    public void markAsOnline() throws IOException
    {
        commitCloseLock.lock();
        try
        {
            luceneIndex.markAsOnline();
        }
        finally
        {
            commitCloseLock.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void markAsFailed( String failure ) throws IOException
    {
        luceneIndex.markAsFailed( failure );
    }

    @Override
    public boolean hasSinglePartition( List<AbstractIndexPartition> partitions )
    {
        return luceneIndex.hasSinglePartition( partitions );
    }

    @Override
    public AbstractIndexPartition getFirstPartition( List<AbstractIndexPartition> partitions )
    {
        return luceneIndex.getFirstPartition( partitions );
    }
}
