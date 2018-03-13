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

import java.io.IOException;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.impl.fulltext.integrations.kernel.FulltextIndexDescriptor;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.IndexStorageFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;
import org.neo4j.kernel.configuration.Config;

public class FulltextFactory
{
    private final WritableIndexPartitionFactory partitionFactory;
    private final IndexStorageFactory indexStorageFactory;
    private final Analyzer analyzer;
    private final Config config;

    /**
     * Creates a factory for the specified location and analyzer.
     *
     * @param analyzerClassName The Lucene analyzer to use for the {@link LuceneFulltext} created by this factory.
     * @param config the config is used for checking the {@link GraphDatabaseSettings#archive_failed_index} setting.
     */

    public FulltextFactory( IndexStorageFactory indexStorageFactory, String analyzerClassName, Config config )
    {
        this.indexStorageFactory = indexStorageFactory;
        this.analyzer = getAnalyzer( analyzerClassName );
        this.config = config;
        Factory<IndexWriterConfig> indexWriterConfigFactory = () -> IndexWriterConfigs.standard( analyzer );
        this.partitionFactory = new WritableIndexPartitionFactory( indexWriterConfigFactory );
    }

    private Analyzer getAnalyzer( String analyzerClassName )
    {
        Analyzer analyzer;
        try
        {
            Class configuredAnalyzer = Class.forName( analyzerClassName );
            analyzer = (Analyzer) configuredAnalyzer.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create the configured analyzer", e );
        }
        return analyzer;
    }

    public LuceneFulltext createFulltextIndex( long indexId, FulltextIndexDescriptor descriptor )
    {
        PartitionedIndexStorage storage = getIndexStorage( indexId );
        return new LuceneFulltext( storage, partitionFactory, descriptor.propertyNames(), analyzer, descriptor.identifier(), descriptor.schema().entityType() );
    }

    public String getStoredIndexFailure( long indexId )
    {
        return getIndexStorage( indexId ).getStoredIndexFailure();
    }

    private PartitionedIndexStorage getIndexStorage( long indexId )
    {
        return indexStorageFactory.indexStorageOf( indexId, config.get( GraphDatabaseSettings.archive_failed_index ) );
    }
}
