/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.api.impl.schema.writer.PartitionedIndexWriter;

class LuceneFulltext extends AbstractLuceneIndex
{
    private final Analyzer analyzer;
    private final String identifier;
    private final FulltextIndexType type;
    private Set<String> properties;
    private volatile InternalIndexState state;

    LuceneFulltext( PartitionedIndexStorage indexStorage, IndexPartitionFactory partitionFactory, Collection<String> properties, Analyzer analyzer,
            String identifier,
                    FulltextIndexType type )
    {
        super( indexStorage, partitionFactory );
        this.properties = Collections.unmodifiableSet( new HashSet<>( properties ) );
        this.analyzer = analyzer;
        this.identifier = identifier;
        this.type = type;
        state = InternalIndexState.POPULATING;
    }

    LuceneFulltext( PartitionedIndexStorage indexStorage, WritableIndexPartitionFactory partitionFactory, Analyzer analyzer, String identifier,
            FulltextIndexType type ) throws IOException
    {
        this( indexStorage, partitionFactory, Collections.EMPTY_SET, analyzer, identifier, type );
        this.properties = readProperties();
    }

    private Set<String> readProperties() throws IOException
    {
        Set<String> props;
        open();
        FulltextIndexConfiguration configurationDocument;
        try ( ReadOnlyFulltext indexReader = getIndexReader() )
        {
            configurationDocument = indexReader.getConfigurationDocument();
        }
        if ( configurationDocument != null )
        {
            props = Collections.unmodifiableSet( configurationDocument.getProperties() );
        }
        else
        {
            props = Collections.emptySet();
        }
        close();
        return props;
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

    ReadOnlyFulltext getIndexReader() throws IOException
    {
        ensureOpen();
        List<AbstractIndexPartition> partitions = getPartitions();
        return hasSinglePartition( partitions ) ? createSimpleReader( partitions ) : createPartitionedReader( partitions );
    }

    FulltextIndexType getType()
    {
        return type;
    }

    Set<String> getProperties()
    {
        return properties;
    }

    String getIdentifier()
    {
        return identifier;
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

    void saveConfiguration( long txId ) throws IOException
    {
        PartitionedIndexWriter writer = getIndexWriter( new WritableFulltext( this ) );
        String analyzerName = analyzer.getClass().getCanonicalName();
        FulltextIndexConfiguration config = new FulltextIndexConfiguration( analyzerName, properties, txId );
        writer.updateDocument( FulltextIndexConfiguration.TERM, config.asDocument() );
    }

    String getAnalyzerName()
    {
        return analyzer.getClass().getCanonicalName();
    }

    InternalIndexState getState()
    {
        return state;
    }

    void setPopulated()
    {
        state = InternalIndexState.ONLINE;
    }

    void setFailed()
    {
        state = InternalIndexState.FAILED;
    }
}
