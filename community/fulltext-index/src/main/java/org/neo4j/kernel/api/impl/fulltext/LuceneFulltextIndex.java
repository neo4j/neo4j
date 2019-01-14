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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import org.neo4j.internal.kernel.api.schema.SchemaUtil;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.PartitionSearcher;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.storageengine.api.EntityType;

public class LuceneFulltextIndex extends AbstractLuceneIndex<FulltextIndexReader> implements Closeable
{
    private final Analyzer analyzer;
    private final String identifier;
    private final EntityType type;
    private final Collection<String> properties;
    private final TokenHolder propertyKeyTokenHolder;
    private final File transactionsFolder;

    LuceneFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, FulltextIndexDescriptor descriptor,
            TokenHolder propertyKeyTokenHolder )
    {
        super( storage, partitionFactory, descriptor );
        this.analyzer = descriptor.analyzer();
        this.identifier = descriptor.getName();
        this.type = descriptor.schema().entityType();
        this.properties = descriptor.propertyNames();
        this.propertyKeyTokenHolder = propertyKeyTokenHolder;
        File indexFolder = storage.getIndexFolder();
        transactionsFolder = new File( indexFolder.getParent(), indexFolder.getName() + ".tx" );
    }

    @Override
    public void open() throws IOException
    {
        super.open();
        indexStorage.prepareFolder( transactionsFolder );
    }

    @Override
    public void close() throws IOException
    {
        super.close();
        indexStorage.cleanupFolder( transactionsFolder );
    }

    @Override
    public String toString()
    {
        return "LuceneFulltextIndex{" +
               "analyzer=" + analyzer.getClass().getSimpleName() +
               ", identifier='" + identifier + '\'' +
               ", type=" + type +
               ", properties=" + properties +
               ", descriptor=" + descriptor.userDescription( SchemaUtil.idTokenNameLookup ) +
               '}';
    }

    String[] getPropertiesArray()
    {
        return properties.toArray( new String[0] );
    }

    Analyzer getAnalyzer()
    {
        return analyzer;
    }

    TokenHolder getPropertyKeyTokenHolder()
    {
        return propertyKeyTokenHolder;
    }

    @Override
    protected FulltextIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        AbstractIndexPartition singlePartition = getFirstPartition( partitions );
        SearcherReference searcher = new PartitionSearcherReference( singlePartition.acquireSearcher() );
        return new SimpleFulltextIndexReader( searcher, getPropertiesArray(), analyzer, propertyKeyTokenHolder );
    }

    @Override
    protected FulltextIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<PartitionSearcher> searchers = acquireSearchers( partitions );
        return new PartitionedFulltextIndexReader( searchers, getPropertiesArray(), analyzer, propertyKeyTokenHolder );
    }
}
