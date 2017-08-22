/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.bloom;

import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.index.IndexWriterConfig;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;

public class BloomIndex implements AutoCloseable
{
    private final BloomLuceneIndex nodeIndex;
    private final BloomLuceneIndex relationshipIndex;
    private final String[] properties;

    public BloomIndex( FileSystemAbstraction fileSystem, File file, Config config ) throws IOException
    {
        this.properties = config.get( GraphDatabaseSettings.bloom_indexed_properties ).toArray( new String[0] );
        EnglishAnalyzer analyzer = new EnglishAnalyzer();
        Factory<IndexWriterConfig> population = () -> {
            return IndexWriterConfigs.population( analyzer );
        };
        WritableIndexPartitionFactory partitionFactory = new WritableIndexPartitionFactory( population );

        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexIdentifier( "insightNodes" )
                .withDirectoryFactory( directoryFactory( false, fileSystem ) )
                .withIndexRootFolder( Paths.get( file.getAbsolutePath(),"insightindex" ).toFile() );
        nodeIndex = new BloomLuceneIndex( storageBuilder.build(), partitionFactory, this.properties, analyzer);
        nodeIndex.open();

        storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexIdentifier( "insightRelationships" )
                .withDirectoryFactory( directoryFactory( false, fileSystem ) )
                .withIndexRootFolder( Paths.get( file.getAbsolutePath(),"insightindex" ).toFile() );
        relationshipIndex = new BloomLuceneIndex( storageBuilder.build(), partitionFactory, properties,  analyzer);
        relationshipIndex.open();
    }

    public BloomIndexTransactionEventUpdater getUpdater() throws IOException
    {
        WritableDatabaseBloomIndex writableNodeIndex = new WritableDatabaseBloomIndex( nodeIndex );
        WritableDatabaseBloomIndex writableRelationshipIndex = new WritableDatabaseBloomIndex( relationshipIndex );
        return new BloomIndexTransactionEventUpdater( writableNodeIndex, writableRelationshipIndex, properties );
    }

    public BloomIndexReader getNodeReader() throws IOException
    {
        return nodeIndex.getIndexReader();
    }

    public BloomIndexReader getRelationshipReader() throws IOException
    {
        return relationshipIndex.getIndexReader();
    }

    @Override
    public void close() throws Exception {
        nodeIndex.close();
        relationshipIndex.close();
    }
}
