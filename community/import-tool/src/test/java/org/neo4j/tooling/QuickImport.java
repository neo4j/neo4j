/*
 * Copyright (c) "Neo4j"
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

import picocli.CommandLine;

import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.neo4j.cli.Converters.ByteUnitConverter;
import org.neo4j.configuration.Config;
import org.neo4j.csv.reader.CharSeeker;
import org.neo4j.csv.reader.CharSeekers;
import org.neo4j.csv.reader.Configuration;
import org.neo4j.csv.reader.Extractors;
import org.neo4j.csv.reader.Readables;
import org.neo4j.internal.batchimport.BatchImporter;
import org.neo4j.internal.batchimport.IndexConfig;
import org.neo4j.internal.batchimport.input.Collector;
import org.neo4j.internal.batchimport.input.DataGeneratorInput;
import org.neo4j.internal.batchimport.input.Groups;
import org.neo4j.internal.batchimport.input.IdType;
import org.neo4j.internal.batchimport.input.Input;
import org.neo4j.internal.batchimport.input.csv.DataFactories;
import org.neo4j.internal.batchimport.input.csv.Header;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.EmptyVersionContextSupplier;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static java.lang.System.currentTimeMillis;
import static org.neo4j.configuration.SettingValueParsers.parseLongWithUnit;
import static org.neo4j.internal.batchimport.AdditionalInitialIds.EMPTY;
import static org.neo4j.internal.batchimport.Configuration.calculateMaxMemoryFromPercent;
import static org.neo4j.internal.batchimport.Configuration.defaultConfiguration;
import static org.neo4j.internal.batchimport.Monitor.NO_MONITOR;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

/**
 * Uses all available shortcuts to as quickly as possible import as much data as possible. Usage of this
 * utility is most likely just testing behavior of some components in the face of various dataset sizes,
 * even quite big ones. Uses the import tool, or rather directly the {@link BatchImporter}.
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
@CommandLine.Command( name = "quick-import", description = "Imports a rather silly data set of an arbitrary size and shape into a database, quickly." )
public class QuickImport implements Callable<Void>
{
    @CommandLine.Option( names = "--into", description = "Target database directory" )
    private Path dir;

    @CommandLine.Option( names = "--nodes", description = "Number of nodes to import, can have post-fix like 10M for 10 million etc.",
            converter = ByteUnitConverter.class, arity = "1" )
    private long nodeCount;

    @CommandLine.Option( names = "--relationships", description = "Number of relationships to import, can have post-fix like 10M for 10 million etc.",
            converter = ByteUnitConverter.class, arity = "1" )
    private long relationshipCount;

    @CommandLine.Option( names = "--labels", description = "Number of different labels in the data set. " +
            "The likelihood of selecting a label is reduced the higher the 'ID', i.e. specifying 3 labels, " +
            "the chance of selecting the first is 50%, the second is 25% and the third 12,5% a.s.o.",
            defaultValue = "4" )
    private int labelCount;

    @CommandLine.Option( names = "--relationship-types", description = "Number of different relationship types in the data set. " +
            "The likelihood of selecting a type is reduced the higher the 'ID', i.e. specifying 3 relationship types, " +
            "the chance of selecting the first is 50%, the second is 25% and the third 12,5% a.s.o.",
            defaultValue = "4" )
    private int relationshipTypeCount;

    @CommandLine.Option( names = "--random-seed", description = "Specific seed to use for the random data generator" )
    private Long randomSeed;

    @CommandLine.Option( names = "--id-type", description = "The type of IDs in the input data, e.g. how the importer handles node ID mappings",
            defaultValue = "INTEGER" )
    private IdType idType;

    @CommandLine.Option( names = "--high-io", description = "Whether or not to force high-IO setting, which is otherwise figured out automatically " +
            "based on the target file system" )
    private Boolean highIO;

    @CommandLine.Option( names = "--pagecache-memory", description = "Specifically configure the page cache memory, other than a reasonable default",
            converter = ByteUnitConverter.class )
    private Long pageCacheMemory;

    @CommandLine.Option( names = "--node-header", description = "Specification of what data the nodes will have, CSV-style like ':ID,:LABEL,name'" )
    private String nodeHeader;

    @CommandLine.Option( names = "--relationship-header", description = "Specification of what data the relationships will have, CSV-style like " +
            "':START_ID,:TYPE,:END_ID,data:long'" )
    private String relationshipHeader;

    @CommandLine.Option( names = "--db-config", description = "File containing additional configuration to load for the import" )
    private Path dbConfigPath;

    @CommandLine.Option( names = "--processors", description = "Limit of the number of processors/cpus to use in the import" )
    private Integer processors;

    @CommandLine.Option( names = "--max-memory", description = "Limit of the maximum amount of off-heap memory the import is allowed to use",
            converter = MaxMemoryConverter.class )
    private Long maxMemory;

    @CommandLine.Option( names = "--factor-bad-node-data", description = "Factor (between 0..1) of nodes that have bad data in them",
            defaultValue = "0" )
    private float factorBadNodeData;

    @CommandLine.Option( names = "--factor-bad-relationship-data", description = "Factor (between 0..1) of relationships that have bad data in them",
            defaultValue = "0" )
    private float factorBadRelationshipData;

    @CommandLine.Option( names = "--to-csv", description = "Instead of importing the generated data, dump it as CSV file(s)" )
    private boolean toCsv;

    @CommandLine.Option( names = "--verbose", description = "Whether or not the output should be verbose" )
    private boolean verbose;

    @CommandLine.Option( names = "--storage-engine", description = "Which storage engine the target database should be" )
    private String storageEngine;

    public static void main( String[] args )
    {
        System.exit( new CommandLine( new QuickImport() ).execute( args ) );
    }

    @Override
    public Void call() throws Exception
    {
        long randomSeed = this.randomSeed != null ? this.randomSeed : currentTimeMillis();
        Configuration config = Configuration.COMMAS;
        Extractors extractors = new Extractors( config.arrayDelimiter() );
        Groups groups = new Groups();
        Header nodeHeader = parseNodeHeader( this.nodeHeader, idType, extractors, groups );
        Header relationshipHeader = parseRelationshipHeader( this.relationshipHeader, idType, extractors, groups );
        Config dbConfig = dbConfigPath != null ? Config.newBuilder().fromFile( dbConfigPath ).build() : Config.defaults();
        LogProvider logging = NullLogProvider.getInstance();
        long pageCacheMemory = this.pageCacheMemory != null ? this.pageCacheMemory : org.neo4j.internal.batchimport.Configuration.MAX_PAGE_CACHE_MEMORY;
        org.neo4j.internal.batchimport.Configuration importConfig = new org.neo4j.internal.batchimport.Configuration.Overridden( defaultConfiguration() )
        {
            @Override
            public int maxNumberOfProcessors()
            {
                return processors != null ? processors : super.maxNumberOfProcessors();
            }

            @Override
            public boolean highIO()
            {
                return highIO != null ? highIO : super.highIO();
            }

            @Override
            public long pageCacheMemory()
            {
                return pageCacheMemory;
            }

            @Override
            public long maxMemoryUsage()
            {
                return maxMemory != null ? maxMemory : super.maxMemoryUsage();
            }

            @Override
            public IndexConfig indexConfig()
            {
                return IndexConfig.create().withLabelIndex().withRelationshipTypeIndex();
            }
        };

        Input input = new DataGeneratorInput(
                nodeCount, relationshipCount,
                idType, randomSeed,
                0, nodeHeader, relationshipHeader, labelCount, relationshipTypeCount,
                factorBadNodeData, factorBadRelationshipData );

        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
                Lifespan life = new Lifespan() )
        {
            BatchImporter consumer;
            if ( toCsv )
            {
                consumer = new CsvOutput( dir, nodeHeader, relationshipHeader, config );
            }
            else
            {
                System.out.println( "Seed " + randomSeed );
                final JobScheduler jobScheduler = life.add( createScheduler() );
                StorageEngineFactory storageEngineFactory = storageEngine != null
                                                            ? StorageEngineFactory.selectStorageEngine( storageEngine )
                                                            : StorageEngineFactory.defaultStorageEngine();
                var contextFactory = new CursorContextFactory( PageCacheTracer.NULL, EmptyVersionContextSupplier.EMPTY );
                consumer = storageEngineFactory.batchImporter( DatabaseLayout.ofFlat( dir ), fileSystem, PageCacheTracer.NULL, importConfig,
                        new SimpleLogService( logging, logging ), System.out, verbose, EMPTY, dbConfig, NO_MONITOR, jobScheduler, Collector.EMPTY,
                        TransactionLogInitializer.getLogFilesInitializer(), new IndexImporterFactoryImpl( dbConfig ), INSTANCE, contextFactory );
            }
            consumer.doImport( input );
        }
        return null;
    }

    private static class MaxMemoryConverter implements CommandLine.ITypeConverter<Long>
    {
        @Override
        public Long convert( String maxMemoryString ) throws Exception
        {
            if ( maxMemoryString != null )
            {
                maxMemoryString = maxMemoryString.trim();
                if ( maxMemoryString.endsWith( "%" ) )
                {
                    int percent = Integer.parseInt( maxMemoryString.substring( 0, maxMemoryString.length() - 1 ) );
                    return calculateMaxMemoryFromPercent( percent );
                }
                return parseLongWithUnit( maxMemoryString );
            }
            return null;
        }
    }

    private Header parseNodeHeader( String definition, IdType idType, Extractors extractors, Groups groups )
    {
        if ( definition == null )
        {
            return DataGeneratorInput.bareboneNodeHeader( idType, extractors );
        }

        Configuration config = Configuration.COMMAS;
        return DataFactories.defaultFormatNodeFileHeader().create( seeker( definition, config ), config, idType, groups );
    }

    private static Header parseRelationshipHeader( String definition, IdType idType, Extractors extractors, Groups groups )
    {
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
