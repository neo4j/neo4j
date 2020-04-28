/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.commandline.dbms;

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings.TransactionStateMemoryAllocation;
import org.neo4j.configuration.SettingImpl;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.api.impl.index.storage.FailureStorage;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.RandomValues;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.bytesToString;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendHeapMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendOsMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendPageCacheMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendTxStateMemory;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.ExternalSettings.initialHeapSize;
import static org.neo4j.configuration.ExternalSettings.maxHeapSize;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SchemaIndex;
import static org.neo4j.configuration.GraphDatabaseSettings.data_directory;
import static org.neo4j.configuration.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.configuration.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_max_off_heap_memory;
import static org.neo4j.configuration.GraphDatabaseSettings.tx_state_memory_allocation;
import static org.neo4j.configuration.SettingValueParsers.BYTES;
import static org.neo4j.internal.helpers.collection.MapUtil.store;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.exbiBytes;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;

@Neo4jLayoutExtension
class MemoryRecommendationsCommandTest
{
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void printUsageHelp()
    {
        final var baos = new ByteArrayOutputStream();
        final var command = new MemoryRecommendationsCommand( new ExecutionContext( Path.of( "." ), Path.of( "." ) ) );
        try ( var out = new PrintStream( baos ) )
        {
            CommandLine.usage( command, new PrintStream( out ) );
        }
        assertThat( baos.toString().trim() ).isEqualTo( String.format(
                "Print Neo4j heap and pagecache memory settings recommendations.%n" +
                "%n" +
                "USAGE%n" +
                "%n" +
                "memrec [--verbose] [--memory=<size>]%n" +
                "%n" +
                "DESCRIPTION%n" +
                "%n" +
                "Print heuristic memory setting recommendations for the Neo4j JVM heap and%n" +
                "pagecache. The heuristic is based on the total memory of the system the command%n" +
                "is running on, or on the amount of memory specified with the --memory argument.%n" +
                "The heuristic assumes that the system is dedicated to running Neo4j. If this is%n" +
                "not the case, then use the --memory argument to specify how much memory can be%n" +
                "expected to be dedicated to Neo4j. The output is formatted such that it can be%n" +
                "copy-pasted into the neo4j.conf file.%n" +
                "%n" +
                "OPTIONS%n" +
                "%n" +
                "      --verbose         Enable verbose output.%n" +
                "      --memory=<size>   Recommend memory settings with respect to the given%n" +
                "                          amount of memory, instead of the total memory of the%n" +
                "                          system running the command."
        ) );
    }

    @Test
    void mustRecommendOSMemory()
    {
        assertThat( recommendOsMemory( mebiBytes( 100 ) ) ).isBetween( mebiBytes( 65 ), mebiBytes( 75 ) );
        assertThat( recommendOsMemory( gibiBytes( 1 ) ) ).isBetween( mebiBytes( 650 ), mebiBytes( 750 ) );
        assertThat( recommendOsMemory( gibiBytes( 3 ) ) ).isBetween( mebiBytes( 1256 ), mebiBytes( 1356 ) );
        assertThat( recommendOsMemory( gibiBytes( 192 ) ) ).isBetween( gibiBytes( 17 ), gibiBytes( 19 ) );
        assertThat( recommendOsMemory( gibiBytes( 1920 ) ) ).isGreaterThan( gibiBytes( 29 ) );
    }

    @Test
    void mustRecommendHeapMemory()
    {
        assertThat( recommendHeapMemory( mebiBytes( 100 ) ) ).isBetween( mebiBytes( 25 ), mebiBytes( 35 ) );
        assertThat( recommendHeapMemory( gibiBytes( 1 ) ) ).isBetween( mebiBytes( 300 ), mebiBytes( 350 ) );
        assertThat( recommendHeapMemory( gibiBytes( 3 ) ) ).isBetween( mebiBytes( 1256 ), mebiBytes( 1356 ) );
        assertThat( recommendHeapMemory( gibiBytes( 6 ) ) ).isBetween( mebiBytes( 3000 ), mebiBytes( 3200 ) );
        assertThat( recommendHeapMemory( gibiBytes( 192 ) ) ).isBetween( gibiBytes( 30 ), gibiBytes( 32 ) );
        assertThat( recommendHeapMemory( gibiBytes( 1920 ) ) ).isBetween( gibiBytes( 30 ), gibiBytes( 32 ) );
    }

    @Test
    void mustRecommendPageCacheMemoryWithOffHeapTxState()
    {
        assertThat( recommendPageCacheMemory( mebiBytes( 100 ), mebiBytes( 130 ) ) ).isBetween( mebiBytes( 7 ), mebiBytes( 12 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1 ), mebiBytes( 260 ) ) ).isBetween( mebiBytes( 8 ), mebiBytes( 50 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 3 ), mebiBytes( 368 ) ) ).isBetween( mebiBytes( 100 ), mebiBytes( 256 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 6 ), mebiBytes( 780 ) ) ).isBetween( mebiBytes( 100 ), mebiBytes( 256 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 192 ), gibiBytes( 10 ) ) ).isBetween( gibiBytes( 75 ), gibiBytes( 202 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1920 ), gibiBytes( 10 ) ) ).isBetween( gibiBytes( 978 ), gibiBytes( 1900 ) );

        // Also never recommend more than 16 TiB of page cache memory, regardless of how much is available.
        assertThat( recommendPageCacheMemory( exbiBytes( 1 ), gibiBytes( 100 ) ) ).isLessThanOrEqualTo( tebiBytes( 16 ) );
    }

    @Test
    void mustRecommendPageCacheMemoryWithOnHeapTxState()
    {
        assertThat( recommendPageCacheMemory( mebiBytes( 100 ), 0 ) ).isBetween( mebiBytes( 7 ), mebiBytes( 12 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1 ), 0 ) ).isBetween( mebiBytes( 20 ), mebiBytes( 60 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 3 ), 0 ) ).isBetween( mebiBytes( 256 ), mebiBytes( 728 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 6 ), 0 ) ).isBetween( mebiBytes( 728 ), mebiBytes( 1056 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 192 ), 0 ) ).isBetween( gibiBytes( 75 ), gibiBytes( 202 ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1920 ), 0 ) ).isBetween( gibiBytes( 978 ), gibiBytes( 1900 ) );

        // Also never recommend more than 16 TiB of page cache memory, regardless of how much is available.
        assertThat( recommendPageCacheMemory( exbiBytes( 1 ), gibiBytes( 100 ) ) ).isLessThanOrEqualTo( tebiBytes( 16 ) );
    }

    @Test
    void recommendTxStatMemory()
    {
        final Config config = Config.defaults();
        assertEquals( mebiBytes( 128 ), recommendTxStateMemory( config, mebiBytes( 100 ) ) );
        assertEquals( mebiBytes( 128 ), recommendTxStateMemory( config, mebiBytes( 512 ) ) );
        assertEquals( mebiBytes( 192 ), recommendTxStateMemory( config, mebiBytes( 768 ) ) );
        assertEquals( mebiBytes( 256 ), recommendTxStateMemory( config, gibiBytes( 1 ) ) );
        assertEquals( gibiBytes( 4 ), recommendTxStateMemory( config, gibiBytes( 16 ) ) );
        assertEquals( gibiBytes( 8 ), recommendTxStateMemory( config, gibiBytes( 32 ) ) );
        assertEquals( gibiBytes( 8 ), recommendTxStateMemory( config, gibiBytes( 128 ) ) );
    }

    @Test
    void bytesToStringMustBeParseableBySettings()
    {
        SettingImpl<Long> setting = (SettingImpl<Long>) SettingImpl.newBuilder( "arg", BYTES, null ).build();
        for ( int i = 1; i < 10_000; i++ )
        {
            int mebibytes = 75 * i;
            long expectedBytes = mebiBytes( mebibytes );
            String bytesToString = bytesToString( expectedBytes );
            long actualBytes = setting.parse( bytesToString );
            long tenPercent = (long) (expectedBytes * 0.1);
            assertThat( actualBytes ).as( mebibytes + "m" ).isBetween( expectedBytes - tenPercent, expectedBytes + tenPercent );
        }
    }

    @Test
    void mustPrintRecommendationsAsConfigReadableOutput() throws Exception
    {
        PrintStream output = mock( PrintStream.class );
        Path homeDir = testDirectory.homeDir().toPath();
        Path configDir = homeDir.resolve( "conf" );
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        configDir.toFile().mkdirs();
        store( stringMap( data_directory.name(), homeDir.toString() ), configFile.toFile() );

        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand(
                new ExecutionContext( homeDir, configDir, output, mock( PrintStream.class ), testDirectory.getFileSystem() ) );

        CommandLine.populateCommand( command, "--memory=8g" );
        String heap = bytesToString( recommendHeapMemory( gibiBytes( 8 ) ) );
        String pagecache = bytesToString( recommendPageCacheMemory( gibiBytes( 8 ), gibiBytes( 2 ) ) );
        String offHeap = bytesToString( gibiBytes( 2 ) );

        command.execute();

        verify( output ).println( initialHeapSize.name() + '=' + heap );
        verify( output ).println( maxHeapSize.name() + '=' + heap );
        verify( output ).println( pagecache_memory.name() + '=' + pagecache );
        verify( output ).println( tx_state_max_off_heap_memory.name() + '=' + offHeap );
    }

    @Test
    void doNotPrintRecommendationsForOffHeapWhenOnHeapIsConfigured() throws Exception
    {
        PrintStream output = mock( PrintStream.class );
        Path homeDir = testDirectory.homeDir().toPath();
        Path configDir = homeDir.resolve( "conf" );
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        configDir.toFile().mkdirs();
        store( stringMap( data_directory.name(), homeDir.toString(),
                tx_state_memory_allocation.name(), TransactionStateMemoryAllocation.ON_HEAP.name() ), configFile.toFile() );

        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand(
                new ExecutionContext( homeDir, configDir, output, mock( PrintStream.class ), testDirectory.getFileSystem() ) );

        CommandLine.populateCommand( command, "--memory=8g" );
        String heap = bytesToString( recommendHeapMemory( gibiBytes( 8 ) ) );
        String pagecache = bytesToString( recommendPageCacheMemory( gibiBytes( 8 ), 0 ) );
        String offHeap = bytesToString( gibiBytes( 2 ) );

        command.execute();

        verify( output ).println( initialHeapSize.name() + '=' + heap );
        verify( output ).println( maxHeapSize.name() + '=' + heap );
        verify( output ).println( pagecache_memory.name() + '=' + pagecache );
        verify( output, never() ).println( tx_state_max_off_heap_memory.name() + '=' + offHeap );
    }

    @Test
    void shouldPrintKilobytesEvenForByteSizeBelowAKiloByte()
    {
        // given
        long bytesBelowK = 176;
        long bytesBelow10K = 1762;
        long bytesBelow100K = 17625;

        // when
        String stringBelowK = MemoryRecommendationsCommand.bytesToString( bytesBelowK );
        String stringBelow10K = MemoryRecommendationsCommand.bytesToString( bytesBelow10K );
        String stringBelow100K = MemoryRecommendationsCommand.bytesToString( bytesBelow100K );

        // then
        assertThat( stringBelowK ).isEqualTo( "1k" );
        assertThat( stringBelow10K ).isEqualTo( "2k" );
        assertThat( stringBelow100K ).isEqualTo( "18k" );
    }

    @Test
    void mustPrintMinimalPageCacheMemorySettingForConfiguredDb() throws Exception
    {
        // given
        Path homeDir = testDirectory.homeDir().toPath();
        Path configDir = homeDir.resolve( "conf" );
        configDir.toFile().mkdirs();
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        String databaseName = "mydb";
        store( stringMap( data_directory.name(), homeDir.toString() ), configFile.toFile() );
        DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( databaseName );
        DatabaseLayout systemLayout = neo4jLayout.databaseLayout( SYSTEM_DATABASE_NAME );
        createDatabaseWithNativeIndexes( databaseLayout );
        PrintStream output = mock( PrintStream.class );
        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand(
                new ExecutionContext( homeDir, configDir, output, mock( PrintStream.class ), testDirectory.getFileSystem() ) );
        String heap = bytesToString( recommendHeapMemory( gibiBytes( 8 ) ) );
        String pagecache = bytesToString( recommendPageCacheMemory( gibiBytes( 8 ), gibiBytes( 2 ) ) );

        // when
        CommandLine.populateCommand( command, "--memory=8g" );
        command.execute();

        // then
        verify( output ).println( contains( initialHeapSize.name() + '=' + heap ) );
        verify( output ).println( contains( maxHeapSize.name() + '=' + heap ) );
        verify( output ).println( contains( pagecache_memory.name() + '=' + pagecache ) );

        long[] expectedSizes = calculatePageCacheFileSize( databaseLayout );
        long[] systemSizes = calculatePageCacheFileSize( systemLayout );
        long expectedPageCacheSize = expectedSizes[0] + systemSizes[0];
        long expectedLuceneSize = expectedSizes[1] + systemSizes[1];

        verify( output ).println( contains( "Total size of lucene indexes in all databases: " + bytesToString( expectedLuceneSize ) ) );
        verify( output ).println( contains( "Total size of data and native indexes in all databases: " + bytesToString( expectedPageCacheSize ) ) );
    }

    @Test
    void includeAllDatabasesToMemoryRecommendations() throws IOException
    {
        PrintStream output = mock( PrintStream.class );
        Path homeDir = testDirectory.homeDir().toPath();
        Path configDir = homeDir.resolve( "conf" );
        configDir.toFile().mkdirs();
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );

        store( stringMap( data_directory.name(), homeDir.toString() ), configFile.toFile() );

        long totalPageCacheSize = 0;
        long totalLuceneIndexesSize = 0;
        for ( int i = 0; i < 5; i++ )
        {
            DatabaseLayout databaseLayout = neo4jLayout.databaseLayout( "db" + i );
            createDatabaseWithNativeIndexes( databaseLayout );
            long[] expectedSizes = calculatePageCacheFileSize( databaseLayout );
            totalPageCacheSize += expectedSizes[0];
            totalLuceneIndexesSize += expectedSizes[1];
        }
        DatabaseLayout systemLayout = neo4jLayout.databaseLayout( SYSTEM_DATABASE_NAME );
        long[] expectedSizes = calculatePageCacheFileSize( systemLayout );
        totalPageCacheSize += expectedSizes[0];
        totalLuceneIndexesSize += expectedSizes[1];

        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand(
                new ExecutionContext( homeDir, configDir, output, mock( PrintStream.class ), testDirectory.getFileSystem() ) );

        CommandLine.populateCommand( command, "--memory=8g" );

        command.execute();

        final long expectedLuceneIndexesSize = totalLuceneIndexesSize;
        final long expectedPageCacheSize = totalPageCacheSize;
        verify( output ).println( contains( "Total size of lucene indexes in all databases: " + bytesToString( expectedLuceneIndexesSize ) ) );
        verify( output ).println( contains( "Total size of data and native indexes in all databases: " + bytesToString( expectedPageCacheSize ) ) );
    }

    private static long[] calculatePageCacheFileSize( DatabaseLayout databaseLayout ) throws IOException
    {
        MutableLong pageCacheTotal = new MutableLong();
        MutableLong luceneTotal = new MutableLong();
        for ( StoreType storeType : StoreType.values() )
        {
            long length = databaseLayout.file( storeType.getDatabaseFile() ).length();
            pageCacheTotal.add( length );
        }

        File indexFolder = IndexDirectoryStructure.baseSchemaIndexFolder( databaseLayout.databaseDirectory() );
        if ( indexFolder.exists() )
        {
            Files.walkFileTree( indexFolder.toPath(), new SimpleFileVisitor<>()
            {
                @Override
                public FileVisitResult visitFile( Path path, BasicFileAttributes attrs )
                {
                    File file = path.toFile();
                    Path name = path.getName( path.getNameCount() - 3 );
                    boolean isLuceneFile = (path.getNameCount() >= 3 && name.toString().startsWith( "lucene-" )) ||
                            (path.getNameCount() >= 4 && path.getName( path.getNameCount() - 4 ).toString().equals( "lucene" ));
                    if ( !FailureStorage.DEFAULT_FAILURE_FILE_NAME.equals( file.getName() ) )
                    {
                        (isLuceneFile ? luceneTotal : pageCacheTotal).add( file.length() );
                    }
                    return FileVisitResult.CONTINUE;
                }
            } );
        }
        pageCacheTotal.add( databaseLayout.labelScanStore().length() );
        return new long[]{pageCacheTotal.longValue(), luceneTotal.longValue()};
    }

    private static void createDatabaseWithNativeIndexes( DatabaseLayout databaseLayout )
    {
        // Create one index for every provider that we have
        for ( SchemaIndex schemaIndex : SchemaIndex.values() )
        {
            DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( databaseLayout.databaseDirectory() ).setConfig(
                            default_schema_provider, schemaIndex.providerName() ).build();
            GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
            String key = "key-" + schemaIndex.name();
            try
            {
                Label labelOne = Label.label( "one" );
                try ( Transaction tx = db.beginTx() )
                {
                    tx.schema().indexFor( labelOne ).on( key ).create();
                    tx.commit();
                }

                try ( Transaction tx = db.beginTx() )
                {
                    RandomValues randomValues = RandomValues.create();
                    for ( int i = 0; i < 10_000; i++ )
                    {
                        tx.createNode( labelOne ).setProperty( key, randomValues.nextValue().asObject() );
                    }
                    tx.commit();
                }
            }
            finally
            {
                managementService.shutdown();
            }
        }
    }
}
