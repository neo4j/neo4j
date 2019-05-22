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
import java.util.Arrays;
import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.kernel.api.impl.index.AbstractLuceneIndex;
import org.neo4j.kernel.api.impl.index.SearcherReference;
import org.neo4j.kernel.api.impl.index.partition.AbstractIndexPartition;
import org.neo4j.kernel.api.impl.index.partition.IndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.token.api.TokenHolder;

import static org.neo4j.common.TokenNameLookup.idTokenNameLookup;

public class LuceneFulltextIndex extends AbstractLuceneIndex<FulltextIndexReader> implements Closeable
{
    private final Analyzer analyzer;
    private final String identifier;
    private final EntityType type;
    private final TokenHolder propertyKeyTokenHolder;
    private final File transactionsFolder;
    private final FulltextIndexDescriptor descriptor;

    LuceneFulltextIndex( PartitionedIndexStorage storage, IndexPartitionFactory partitionFactory, FulltextIndexDescriptor descriptor,
            TokenHolder propertyKeyTokenHolder )
    {
        super( storage, partitionFactory, descriptor );
        this.descriptor = descriptor;
        this.analyzer = descriptor.analyzer();
        this.identifier = descriptor.name();
        this.type = descriptor.schema().entityType();
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
               ", properties=" + Arrays.toString( descriptor.propertyNames() ) +
               ", descriptor=" + descriptor.userDescription( idTokenNameLookup ) +
               '}';
    }

    Analyzer getAnalyzer()
    {
        return analyzer;
    }

    @Override
    public FulltextIndexDescriptor getDescriptor()
    {
        return descriptor;
    }

    @Override
    protected FulltextIndexReader createSimpleReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        return createPartitionedReader( partitions );
    }

    @Override
    protected FulltextIndexReader createPartitionedReader( List<AbstractIndexPartition> partitions ) throws IOException
    {
        List<SearcherReference> searchers = acquireSearchers( partitions );
        return new FulltextIndexReader( searchers, propertyKeyTokenHolder, getDescriptor() );
    }
}
