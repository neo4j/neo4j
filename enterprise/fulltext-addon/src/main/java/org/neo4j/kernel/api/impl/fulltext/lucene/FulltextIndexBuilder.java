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
import org.apache.lucene.index.IndexWriterConfig;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.impl.fulltext.integrations.kernel.FulltextIndexDescriptor;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.AbstractLuceneIndexBuilder;
import org.neo4j.kernel.api.impl.index.partition.ReadOnlyIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.configuration.Config;

public class FulltextIndexBuilder extends AbstractLuceneIndexBuilder<FulltextIndexBuilder>
{
    private final FulltextIndexDescriptor descriptor;
    private final Analyzer analyzer;
    private Factory<IndexWriterConfig> writerConfigFactory = IndexWriterConfigs::standard;

    private FulltextIndexBuilder( FulltextIndexDescriptor descriptor, Config config, Analyzer analyzer )
    {
        super( config );
        this.descriptor = descriptor;
        this.analyzer = analyzer;
    }

    /**
     * Create new lucene fulltext index builder.
     *
     * @param descriptor The descriptor for this index
     * @return new FulltextIndexBuilder
     */
    public static FulltextIndexBuilder create( FulltextIndexDescriptor descriptor, Config config, Analyzer analyzer )
    {
        return new FulltextIndexBuilder( descriptor, config, analyzer );
    }

    /**
     * Specify {@link Factory} of lucene {@link IndexWriterConfig} to create {@link org.apache.lucene.index.IndexWriter}s.
     *
     * @param writerConfigFactory the supplier of writer configs
     * @return index builder
     */
    public FulltextIndexBuilder withWriterConfig( Factory<IndexWriterConfig> writerConfigFactory )
    {
        this.writerConfigFactory = writerConfigFactory;
        return this;
    }

    /**
     * Build lucene schema index with specified configuration
     *
     * @return lucene schema index
     */
    public FulltextIndex build()
    {
        if ( isReadOnly() )
        {

            return new ReadOnlyFulltext( storageBuilder.build(), new ReadOnlyIndexPartitionFactory(), analyzer, descriptor );
        }
        else
        {
            Boolean archiveFailed = getConfig( GraphDatabaseSettings.archive_failed_index );
            PartitionedIndexStorage storage = storageBuilder.archivingFailed( archiveFailed ).build();
            return new WritableFulltext( storage, new WritableIndexPartitionFactory( writerConfigFactory ), analyzer, descriptor );
        }
    }
}
