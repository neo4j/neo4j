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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.neo4j.kernel.api.impl.fulltext.FulltextIndexDescriptor;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.storageengine.api.EntityType;

public class LuceneFulltextIndex extends AbstractLuceneIndex<FulltextIndexReader> implements Closeable
{
    private final Analyzer analyzer;
    private final String identifier;
    private final EntityType type;
    private Collection<String> properties;

    LuceneFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, Analyzer analyzer, FulltextIndexDescriptor descriptor )
    {
        super( storage, partitionFactory, descriptor );
        this.analyzer = analyzer;
        this.identifier = descriptor.getName();
        this.type = descriptor.schema().entityType();
        this.properties = descriptor.propertyNames();
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

        LuceneFulltextIndex that = (LuceneFulltextIndex) o;

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

    @Override
    protected FulltextIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        PartitionSearcher partitionSearcher = singlePartition.acquireSearcher();
        return new SimpleFulltextIndexReader( partitionSearcher, properties.toArray( new String[0] ), analyzer );
    }

    @Override
    protected FulltextIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedFulltextIndexReader( searchers, properties.toArray( new String[0] ), analyzer );
    }
}
