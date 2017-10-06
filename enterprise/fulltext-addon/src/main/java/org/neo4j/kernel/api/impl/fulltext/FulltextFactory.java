/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.IndexWriterConfig;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.neo4j.function.Factory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.api.impl.index.storage.PartitionedIndexStorage;

/**
 * Used for creating {@link LuceneFulltext} and registering those to a {@link FulltextProvider}.
 */
class FulltextFactory
{
    public static final String INDEX_DIR = "bloom_fts";
    private final FileSystemAbstraction fileSystem;
    private final WritableIndexPartitionFactory partitionFactory;
    private final File indexDir;
    private final Analyzer analyzer;

    /**
     * Creates a factory for the specified location and analyzer.
     *
     * @param fileSystem The filesystem to use.
     * @param storeDir Store directory of the database.
     * @param analyzerClassName The Lucene analyzer to use for the {@link LuceneFulltext} created by this factory.
     * @throws IOException
     */
    FulltextFactory( FileSystemAbstraction fileSystem, File storeDir, String analyzerClassName ) throws IOException
    {
        this.analyzer = getAnalyzer( analyzerClassName );
        this.fileSystem = fileSystem;
        Factory<IndexWriterConfig> indexWriterConfigFactory = () -> IndexWriterConfigs.standard( analyzer );
        partitionFactory = new WritableIndexPartitionFactory( indexWriterConfigFactory );
        indexDir = new File( storeDir, INDEX_DIR );
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

    LuceneFulltext createFulltextIndex( String identifier, FulltextIndexType type, List<String> properties ) throws IOException
    {
        File indexRootFolder = new File( indexDir, identifier );
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexFolder( indexRootFolder );
        PartitionedIndexStorage storage = storageBuilder.build();
        return new LuceneFulltext( storage, partitionFactory, properties, analyzer, identifier, type );
    }

    LuceneFulltext openFulltextIndex( String identifier, FulltextIndexType type ) throws IOException
    {
        File indexRootFolder = new File( indexDir, identifier );
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexFolder( indexRootFolder );
        PartitionedIndexStorage storage = storageBuilder.build();
        return new LuceneFulltext( storage, partitionFactory, analyzer, identifier, type );
    }
}
