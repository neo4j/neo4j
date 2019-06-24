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
import java.io.PrintStream;
import java.util.Collection;

import org.neo4j.batchinsert.internal.TransactionLogsInitializer;
import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.CharSeekers;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Readables;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.batchimport.ParallelBatchImporter;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.DataGeneratorInput;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.csv.DataFactories;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.internal.helpers.Args;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.RecordFormatSelector;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.SettingValueParsers.parseLongWithUnit;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.internal.batchimport.ImportLogic.NO_MONITOR;
import static org.neo4j.internal.batchimport.staging.ExecutionMonitors.defaultVisible;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;

/**
 * Uses all available shortcuts to as quickly as possible import as much data as possible. Usage of this
 * utility is most likely just testing behavior of some components in the face of various dataset sizes,
 * even quite big ones. Uses the import tool, or rather directly the {@link ParallelBatchImporter}.
 * <p>
 * Quick comes from gaming terminology where you sometimes just want to play a quick game, without
 * any settings or hazzle, just play.
 * <p>
 * Uses {@link DataGeneratorInput} as random data {@link Input}.
 * <p>
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
        long nodeCount = parseLongWithUnit( args.get( "nodes", null ) );
        long relationshipCount = parseLongWithUnit( args.get( "relationships", null ) );
        int labelCount = args.getNumber( "labels", 4 ).intValue();
        int relationshipTypeCount = args.getNumber( "relationship-types", 4 ).intValue();
        File dir = new File( args.get( "into" ) );
        long randomSeed = args.getNumber( "random-seed", currentTimeMillis() ).longValue();
        Configuration config = Configuration.COMMAS;

        Extractors extractors = new Extractors( config.arrayDelimiter() );
        IdType idType = IdType.valueOf( args.get( "id-type", IdType.INTEGER.name() ) );

        Groups groups = new Groups();
        Header nodeHeader = parseNodeHeader( args, idType, extractors, groups );
        Header relationshipHeader = parseRelationshipHeader( args, idType, extractors, groups );

        Config dbConfig;
        String dbConfigFileName = args.get( "db-config", null );
        if ( dbConfigFileName != null )
        {
            dbConfig = Config.newBuilder().fromFile( new File( dbConfigFileName ) ).build();
        }
        else
        {
            dbConfig = Config.defaults();
        }

        boolean highIo = args.getBoolean( "high-io" );

        LogProvider logging = NullLogProvider.getInstance();
        long pageCacheMemory = args.getNumber( "pagecache-memory",
                org.neo4j.internal.batchimport.Configuration.MAX_PAGE_CACHE_MEMORY ).longValue();
        org.neo4j.internal.batchimport.Configuration importConfig =
                new org.neo4j.internal.batchimport.Configuration()
                {
                    @Override
                    public int maxNumberOfProcessors()
                    {
                        return args.getNumber( "processors", DEFAULT.maxNumberOfProcessors() ).intValue();
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
                        String custom = args.get( "max-memory", null );
                        return custom != null ? parseMaxMemory( custom ) : DEFAULT.maxMemoryUsage();
                    }
                };

        float factorBadNodeData = args.getNumber( "factor-bad-node-data", 0 ).floatValue();
        float factorBadRelationshipData = args.getNumber( "factor-bad-relationship-data", 0 ).floatValue();

        Input input = new DataGeneratorInput(
                nodeCount, relationshipCount,
                idType, randomSeed,
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
                final JobScheduler jobScheduler = life.add( createScheduler() );
                consumer = BatchImporterFactory.withHighestPriority().instantiate( DatabaseLayout.of( dir ), fileSystem, null, importConfig,
                        new SimpleLogService( logging, logging ), defaultVisible(), EMPTY, dbConfig,
                        RecordFormatSelector.selectForConfig( dbConfig, logging ), NO_MONITOR, jobScheduler, Collector.EMPTY,
                        TransactionLogsInitializer.INSTANCE );
            }
            consumer.doImport( input );
        }
    }

    private static void printInputFiles( String name, Collection<Args.Option<File[]>> files, PrintStream out )
    {
        if ( files.isEmpty() )
        {
            return;
        }

        out.println( name + ":" );
        int i = 0;
        for ( Args.Option<File[]> group : files )
        {
            if ( i++ > 0 )
            {
                out.println();
            }
            if ( group.metadata() != null )
            {
                printIndented( ":" + group.metadata(), out );
            }
            for ( File file : group.value() )
            {
                printIndented( file, out );
            }
        }
    }

    private static void printIndented( Object value, PrintStream out )
    {
        out.println( "  " + value );
    }

    private static Long parseMaxMemory( String maxMemoryString )
    {
        if ( maxMemoryString != null )
        {
            maxMemoryString = maxMemoryString.trim();
            if ( maxMemoryString.endsWith( "%" ) )
            {
                int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
                long result = calculateMaxMemoryFromPercent( percent );
                return result;
            }
            return parseLongWithUnit( maxMemoryString );
        }
        return null;
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

    private static CharSeeker seeker( String definition, org.neo4j.csv.reader.Configuration config )
    {
        return CharSeekers.charSeeker( Readables.wrap( definition ), config.toBuilder().withBufferSize( 10_000 ).build(), false );
    }
}
