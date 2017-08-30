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
import java.nio.file.Paths;

import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.impl.index.IndexWriterConfigs;
import org.neo4j.kernel.api.impl.index.builder.LuceneIndexStorageBuilder;
import org.neo4j.kernel.api.impl.index.partition.WritableIndexPartitionFactory;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.kernel.api.impl.index.LuceneKernelExtensions.directoryFactory;

public class FulltextHelperFactory
{
    private FileSystemAbstraction fileSystem;
    private final String[] properties;
    private final WritableIndexPartitionFactory partitionFactory;
    private final Factory<IndexWriterConfig> population;
    private final File storeDir;
    private Analyzer analyzer;

    public LuceneFulltextHelper createFulltextHelper( String identifier, FULLTEXT_HELPER_TYPE type )
    {
        LuceneIndexStorageBuilder storageBuilder = LuceneIndexStorageBuilder.create();
        storageBuilder.withFileSystem( fileSystem ).withIndexIdentifier( identifier ).withDirectoryFactory(
                directoryFactory( false, this.fileSystem ) ).withIndexRootFolder( Paths.get( this.storeDir.getAbsolutePath(), "fulltextHelper" ).toFile() );
        return new LuceneFulltextHelper( storageBuilder.build(), partitionFactory, this.properties, analyzer, identifier, type );
    }

    public enum FULLTEXT_HELPER_TYPE
    {
        NODES,
        RELATIONSHIPS
    }

    public FulltextHelperFactory( FileSystemAbstraction fileSystem, File storeDir, Config config ) throws IOException
    {
        this.fileSystem = fileSystem;
        this.properties = config.get( GraphDatabaseSettings.bloom_indexed_properties ).toArray( new String[0] );
        try
        {
            Class configuredAnalayzer = Class.forName( config.get( GraphDatabaseSettings.bloom_analyzer ) );
            analyzer = (Analyzer) configuredAnalayzer.newInstance();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Could not create the configured analyzer", e );
        }
        population = () -> IndexWriterConfigs.population( analyzer );
        partitionFactory = new WritableIndexPartitionFactory( population );
        this.storeDir = storeDir;
    }
}
