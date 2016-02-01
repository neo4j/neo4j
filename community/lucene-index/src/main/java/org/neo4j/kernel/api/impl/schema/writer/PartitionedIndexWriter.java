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
import java.util.Optional;

import org.neo4j.kernel.api.impl.index.partition.IndexPartition;
import org.neo4j.kernel.api.impl.schema.LuceneSchemaIndex;

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
    private LuceneSchemaIndex index;

    private final Integer MAXIMUM_PARTITION_SIZE = Integer.getInteger( "luceneSchemaIndex.maxPartitionSize",
            Integer.MAX_VALUE - (Integer.MAX_VALUE / 10) );

    public PartitionedIndexWriter( LuceneSchemaIndex index ) throws IOException
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
        List<IndexPartition> partitions = index.getPartitions();
        for ( IndexPartition partition : partitions )
        {
            partition.getIndexWriter().updateDocument( term, doc );
        }
    }

    @Override
    public void deleteDocuments( Query query ) throws IOException
    {
        List<IndexPartition> partitions = index.getPartitions();
        for ( IndexPartition partition : partitions )
        {
            partition.getIndexWriter().deleteDocuments( query );
        }
    }

    @Override
    public void deleteDocuments( Term term ) throws IOException
    {
        List<IndexPartition> partitions = index.getPartitions();
        for ( IndexPartition partition : partitions )
        {
            partition.getIndexWriter().deleteDocuments( term );
        }
    }

    private synchronized IndexWriter getIndexWriter() throws IOException
    {
        List<IndexPartition> indexPartitions = index.getPartitions();
        Optional<IndexPartition> writablePartition = indexPartitions.stream()
                .filter( this::writablePartition )
                .findFirst();
        if ( writablePartition.isPresent() )
        {
            return writablePartition.get().getIndexWriter();
        }
        else
        {
            IndexPartition indexPartition = index.addNewPartition();
            return indexPartition.getIndexWriter();
        }
    }

    private boolean writablePartition( IndexPartition partition )
    {
        return partition.getIndexWriter().numDocs() < MAXIMUM_PARTITION_SIZE;
    }
}

