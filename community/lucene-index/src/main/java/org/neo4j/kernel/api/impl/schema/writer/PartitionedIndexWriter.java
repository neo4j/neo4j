/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.schema.writer;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.schema.WritableDatabaseSchemaIndex;

/**
 * Schema Lucene index writer implementation that supports writing into multiple partitions and creates partitions
 * on-demand if needed.
 * <p>
 * Writer threats partition as writable if partition has number of documents that is less then configured
 * {@link #MAXIMUM_PARTITION_SIZE}.
 * First observable partition that satisfy writer criteria is used for writing.
 */
public class PartitionedIndexWriter implements LuceneIndexWriter
{
    private final WritableDatabaseSchemaIndex index;

    private final Integer MAXIMUM_PARTITION_SIZE = Integer.getInteger( "luceneSchemaIndex.maxPartitionSize",
            Integer.MAX_VALUE - (Integer.MAX_VALUE / 10) );

    public PartitionedIndexWriter( WritableDatabaseSchemaIndex index ) throws IOException
    {
        this.index = index;
    }

    @Override
    public void addDocument( Document doc ) throws IOException
    {
        getIndexWriter().addDocument( doc );
    }

    @Override
    public void addDocuments( Iterable<Document> documents ) throws IOException
    {
        getIndexWriter().addDocuments( documents );
    }

    @Override
    public void updateDocument( Term term, Document doc ) throws IOException
    {
        List<AbstractIndexPartition> partitions = index.getPartitions();
        if ( index.hasSinglePartition( partitions ) )
        {
            index.getFirstPartition( partitions ).getIndexWriter().updateDocument( term, doc );
        }
        else
        {
            deleteDocuments( term );
            addDocument( doc );
        }
    }

    @Override
    public void deleteDocuments( Query query ) throws IOException
    {
        List<AbstractIndexPartition> partitions = index.getPartitions();
        for ( AbstractIndexPartition partition : partitions )
        {
            partition.getIndexWriter().deleteDocuments( query );
        }
    }

    @Override
    public void deleteDocuments( Term term ) throws IOException
    {
        List<AbstractIndexPartition> partitions = index.getPartitions();
        for ( AbstractIndexPartition partition : partitions )
        {
            partition.getIndexWriter().deleteDocuments( term );
        }
    }

    private IndexWriter getIndexWriter() throws IOException
    {
        synchronized ( index )
        {
            // We synchronise on the index to coordinate with all writers about how many partitions we
            // have, and when new ones are created. The discovery that a new partition needs to be added,
            // and the call to index.addNewPartition() must be atomic.
            return unsafeGetIndexWriter();
        }
    }

    private IndexWriter unsafeGetIndexWriter() throws IOException
    {
        List<AbstractIndexPartition> indexPartitions = index.getPartitions();
        int size = indexPartitions.size();
        //noinspection ForLoopReplaceableByForEach
        for ( int i = 0; i < size; i++ )
        {
            // We should find the *first* writable partition, so we can fill holes left by index deletes.
            AbstractIndexPartition partition = indexPartitions.get( i );
            if ( writablePartition( partition ) )
            {
                return partition.getIndexWriter();
            }
        }
        AbstractIndexPartition indexPartition = index.addNewPartition();
        return indexPartition.getIndexWriter();
    }

    private boolean writablePartition( AbstractIndexPartition partition )
    {
        return partition.getIndexWriter().numDocs() < MAXIMUM_PARTITION_SIZE;
    }
}

