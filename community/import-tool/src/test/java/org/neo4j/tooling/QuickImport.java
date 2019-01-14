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
package org.neo4j.tooling;

import java.io.File;
import java.io.IOException;
import java.util.Collections;

import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.CharSeekers;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Readables;
import org.neo4j.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.logging.SimpleLogService;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.impl.scheduler.CentralJobScheduler;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.unsafe.impl.batchimport.BatchImporter;
import org.neo4j.unsafe.impl.batchimport.BatchImporterFactory;
import org.neo4j.unsafe.impl.batchimport.ParallelBatchImporter;
import org.neo4j.unsafe.impl.batchimport.input.Collector;
import org.neo4j.unsafe.impl.batchimport.input.DataGeneratorInput;
import org.neo4j.unsafe.impl.batchimport.input.Groups;
import org.neo4j.unsafe.impl.batchimport.input.Input;
import org.neo4j.unsafe.impl.batchimport.input.csv.Configuration;
import org.neo4j.unsafe.impl.batchimport.input.csv.DataFactories;
import org.neo4j.unsafe.impl.batchimport.input.csv.Header;
import org.neo4j.unsafe.impl.batchimport.input.csv.IdType;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.unsafe.impl.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.unsafe.impl.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.unsafe.impl.batchimport.staging.ExecutionMonitors.defaultVisible;

/**
 * Uses all available shortcuts to as quickly as possible import as much data as possible. Usage of this
 * utility is most likely just testing behavior of some components in the face of various dataset sizes,
 * even quite big ones. Uses the import tool, or rather directly the {@link ParallelBatchImporter}.
 *
 * Quick comes from gaming terminology where you sometimes just want to play a quick game, without
 * any settings or hazzle, just play.
 *
 * Uses {@link DataGeneratorInput} as random data {@link Input}.
 *
 * For the time being the node/relationship data can't be controlled via command-line arguments,
 * only through changing the code. The {@link DataGeneratorInput} accepts two {@link Header headers}
 * describing which sort of data it should generate.
 */
public class QuickImport
{
    private QuickImport()
    {
    }

    public static void main( String[] arguments ) throws IOException
    {
        Args args = Args.parse( arguments );
        long nodeCount = Settings.parseLongWithUnit( args.get( "nodes", null ) );
        long relationshipCount = Settings.parseLongWithUnit( args.get( "relationships", null ) );
        int labelCount = args.getNumber( "labels", 4 ).intValue();
        int relationshipTypeCount = args.getNumber( "relationship-types", 4 ).intValue();
        File dir = new File( args.get( ImportTool.Options.STORE_DIR.key() ) );
        long randomSeed = args.getNumber( "random-seed", currentTimeMillis() ).longValue();
        Configuration config = Configuration.COMMAS;

        Extractors extractors = new Extractors( config.arrayDelimiter() );
        IdType idType = IdType.valueOf( args.get( "id-type", IdType.INTEGER.name() ) );

        Groups groups = new Groups();
        Header nodeHeader = parseNodeHeader( args, idType, extractors, groups );
        Header relationshipHeader = parseRelationshipHeader( args, idType, extractors, groups );

        Config dbConfig;
        String dbConfigFileName = args.get( ImportTool.Options.DATABASE_CONFIG.key(), null );
        if ( dbConfigFileName != null )
        {
            dbConfig = new Config.Builder().withFile( new File( dbConfigFileName ) ).build();
        }
        else
        {
            dbConfig = Config.defaults();
        }

        boolean highIo = args.getBoolean( ImportTool.Options.HIGH_IO.key() );

        LogProvider logging = NullLogProvider.getInstance();
        long pageCacheMemory = args.getNumber( "pagecache-memory",
                org.neo4j.unsafe.impl.batchimport.Configuration.MAX_PAGE_CACHE_MEMORY ).longValue();
        org.neo4j.unsafe.impl.batchimport.Configuration importConfig =
                new org.neo4j.unsafe.impl.batchimport.Configuration()
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return args.getNumber( ImportTool.Options.PROCESSORS.key(), DEFAULT.maxNumberOfProcessors() ).intValue();
            }

            @Override
            public int denseNodeThreshold()
            {
                return args.getNumber( dense_node_threshold.name(), DEFAULT.denseNodeThreshold() ).intValue();
            }

            @Override
            public boolean highIO()
            {
                return highIo;
            }

            @Override
            public long pageCacheMemory()
            {
                return pageCacheMemory;
            }

            @Override
            public long maxMemoryUsage()
            {
                String custom = args.get( ImportTool.Options.MAX_MEMORY.key(), (String) ImportTool.Options.MAX_MEMORY.defaultValue() );
                return custom != null ? ImportTool.parseMaxMemory( custom ) : DEFAULT.maxMemoryUsage();
            }
        };

        float factorBadNodeData = args.getNumber( "factor-bad-node-data", 0 ).floatValue();
        float factorBadRelationshipData = args.getNumber( "factor-bad-relationship-data", 0 ).floatValue();

        Input input = new DataGeneratorInput(
                nodeCount, relationshipCount,
                idType, Collector.EMPTY, randomSeed,
                0, nodeHeader, relationshipHeader, labelCount, relationshipTypeCount,
                factorBadNodeData, factorBadRelationshipData );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              Lifespan life = new Lifespan() )
        {
            BatchImporter consumer;
            if ( args.getBoolean( "to-csv" ) )
            {
                consumer = new CsvOutput( dir, nodeHeader, relationshipHeader, config );
            }
            else
            {
                System.out.println( "Seed " + randomSeed );
                final JobScheduler jobScheduler = life.add( new CentralJobScheduler() );
                consumer = BatchImporterFactory.withHighestPriority().instantiate( dir, fileSystem, null, importConfig,
                        new SimpleLogService( logging, logging ), defaultVisible( jobScheduler ), EMPTY, dbConfig,
                        RecordFormatSelector.selectForConfig( dbConfig, logging ), NO_MONITOR );
                ImportTool.printOverview( dir, Collections.emptyList(), Collections.emptyList(), importConfig, System.out );
            }
            consumer.doImport( input );
        }
    }

    private static Header parseNodeHeader( Args args, IdType idType, Extractors extractors, Groups groups )
    {
        String definition = args.get( "node-header", null );
        if ( definition == null )
        {
            return DataGeneratorInput.bareboneNodeHeader( idType, extractors );
        }

        Configuration config = Configuration.COMMAS;
        return DataFactories.defaultFormatNodeFileHeader().create( seeker( definition, config ), config, idType, groups );
    }

    private static Header parseRelationshipHeader( Args args, IdType idType, Extractors extractors, Groups groups )
    {
        String definition = args.get( "relationship-header", null );
        if ( definition == null )
        {
            return DataGeneratorInput.bareboneRelationshipHeader( idType, extractors );
        }

        Configuration config = Configuration.COMMAS;
        return DataFactories.defaultFormatRelationshipFileHeader().create( seeker( definition, config ), config,
                idType, groups );
    }

    private static CharSeeker seeker( String definition, Configuration config )
    {
        return CharSeekers.charSeeker( Readables.wrap( definition ),
                new org.neo4j.csv.reader.Configuration.Overridden( config )
        {
            @Override
            public int bufferSize()
            {
                return 10_000;
            }
        }, false );
    }
}
