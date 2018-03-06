/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.fulltext.lucene;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;
import org.neo4j.storageengine.api.EntityType;

import static java.util.Collections.singletonMap;

public class LuceneFulltext extends AbstractLuceneIndex implements Closeable
{
    private final Analyzer analyzer;
    private final String identifier;
    private final EntityType type;
    private Set<String> properties;
    private static final String KEY_STATUS = "status";
    private static final String ONLINE = "online";
    private static final Map<String,String> ONLINE_COMMIT_USER_DATA = singletonMap( KEY_STATUS, ONLINE );

    LuceneFulltext( PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory, Collection<String> properties, Analyzer analyzer,
            String identifier, EntityType type )
    {
        super( indexStorage, partitionFactory );
        this.properties = Collections.unmodifiableSet( new HashSet<>( properties ) );
        this.analyzer = analyzer;
        this.identifier = identifier;
        this.type = type;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        LuceneFulltext that = (LuceneFulltext) o;

        if ( !identifier.equals( that.identifier ) )
        {
            return false;
        }
        return type == that.type;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( identifier, type );
    }

    PartitionedIndexWriter getIndexWriter( WritableFulltext writableIndex )
    {
        ensureOpen();
        return new PartitionedIndexWriter( writableIndex );
    }

    public ReadOnlyFulltext getIndexReader() throws IOException
    {
        ensureOpen();
        List<AbstractIndexPartition> partitions = getPartitions();
        return hasSinglePartition( partitions ) ? createSimpleReader( partitions ) : createPartitionedReader( partitions );
    }

    private SimpleFulltextReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        PartitionSearcher partitionSearcher = singlePartition.acquireSearcher();
        return new SimpleFulltextReader( partitionSearcher, properties.toArray( new String[0] ), analyzer );
    }

    private PartitionedFulltextReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedFulltextReader( searchers, properties.toArray( new String[0] ), analyzer );
    }

    /**
     * Check if this index is marked as online.
     *
     * @return <code>true</code> if index is online, <code>false</code> otherwise
     * @throws IOException
     */
    public boolean isOnline() throws IOException
    {
        ensureOpen();
        AbstractIndexPartition partition = getFirstPartition( getPartitions() );
        Directory directory = partition.getDirectory();
        try ( DirectoryReader reader = DirectoryReader.open( directory ) )
        {
            Map<String,String> userData = reader.getIndexCommit().getUserData();
            return ONLINE.equals( userData.get( KEY_STATUS ) );
        }
    }

    /**
     * Marks index as online by including "status" -> "online" map into commit metadata of the first partition.
     *
     * @throws IOException
     */
    public void markAsOnline() throws IOException
    {
        ensureOpen();
        AbstractIndexPartition partition = getFirstPartition( getPartitions() );
        IndexWriter indexWriter = partition.getIndexWriter();
        indexWriter.setCommitData( ONLINE_COMMIT_USER_DATA );
        flush( false );
    }

    /**
     * Writes the given failure message to the failure storage.
     *
     * @param failure the failure message.
     * @throws IOException
     */
    public void markAsFailed( String failure ) throws IOException
    {
        indexStorage.storeIndexFailure( failure );
    }
}
