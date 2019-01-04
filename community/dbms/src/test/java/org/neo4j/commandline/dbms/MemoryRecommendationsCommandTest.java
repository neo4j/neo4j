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
package org.neo4j.commandline.dbms;

import org.apache.commons.lang3.mutable.MutableLong;
import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;

import org.neo4j.commandline.admin.OutsideWorld;
import org.neo4j.commandline.admin.RealOutsideWorld;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.impl.index.storage.FailureStorage;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.bytesToString;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendHeapMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendOsMemory;
import static org.neo4j.commandline.dbms.MemoryRecommendationsCommand.recommendPageCacheMemory;
import static org.neo4j.configuration.ExternalSettings.initialHeapSize;
import static org.neo4j.configuration.ExternalSettings.maxHeapSize;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.active_database;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.data_directory;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.database_path;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.default_schema_provider;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.pagecache_memory;
import static org.neo4j.helpers.ArrayUtil.array;
import static org.neo4j.helpers.collection.MapUtil.store;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.io.ByteUnit.exbiBytes;
import static org.neo4j.io.ByteUnit.gibiBytes;
import static org.neo4j.io.ByteUnit.mebiBytes;
import static org.neo4j.io.ByteUnit.tebiBytes;
import static org.neo4j.kernel.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.kernel.configuration.Settings.BYTES;
import static org.neo4j.kernel.configuration.Settings.buildSetting;
import static org.neo4j.test.TestLabels.LABEL_ONE;

public class MemoryRecommendationsCommandTest
{
    @Rule
    public final TestDirectory directory = TestDirectory.testDirectory();

    @Test
    public void mustRecommendOSMemory()
    {
        assertThat( recommendOsMemory( mebiBytes( 100 ) ), between( mebiBytes( 45 ), mebiBytes( 55 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 1 ) ), between( mebiBytes( 450 ), mebiBytes( 550 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 3 ) ), between( mebiBytes( 1256 ), mebiBytes( 1356 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 192 ) ), between( gibiBytes( 17 ), gibiBytes( 19 ) ) );
        assertThat( recommendOsMemory( gibiBytes( 1920 ) ), greaterThan( gibiBytes( 29 ) ) );
    }

    @Test
    public void mustRecommendHeapMemory()
    {
        assertThat( recommendHeapMemory( mebiBytes( 100 ) ), between( mebiBytes( 45 ), mebiBytes( 55 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 1 ) ), between( mebiBytes( 450 ), mebiBytes( 550 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 3 ) ), between( mebiBytes( 1256 ), mebiBytes( 1356 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 6 ) ), between( mebiBytes( 3000 ), mebiBytes( 3200 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 192 ) ), between( gibiBytes( 30 ), gibiBytes( 32 ) ) );
        assertThat( recommendHeapMemory( gibiBytes( 1920 ) ), between( gibiBytes( 30 ), gibiBytes( 32 ) ) );
    }

    @Test
    public void mustRecommendPageCacheMemory()
    {
        assertThat( recommendPageCacheMemory( mebiBytes( 100 ) ), between( mebiBytes( 95 ), mebiBytes( 105 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1 ) ), between( mebiBytes( 95 ), mebiBytes( 105 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 3 ) ), between( mebiBytes( 470 ), mebiBytes( 530 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 6 ) ), between( mebiBytes( 980 ), mebiBytes( 1048 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 192 ) ), between( gibiBytes( 140 ), gibiBytes( 150 ) ) );
        assertThat( recommendPageCacheMemory( gibiBytes( 1920 ) ), between( gibiBytes( 1850 ), gibiBytes( 1900 ) ) );

        // Also never recommend more than 16 TiB of page cache memory, regardless of how much is available.
        assertThat( recommendPageCacheMemory( exbiBytes( 1 ) ), lessThan( tebiBytes( 17 ) ) );
    }

    @Test
    public void bytesToStringMustBeParseableBySettings()
    {
        Setting<Long> setting = buildSetting( "arg", BYTES ).build();
        for ( int i = 1; i < 10_000; i++ )
        {
            int mebibytes = 75 * i;
            long expectedBytes = mebiBytes( mebibytes );
            String bytesToString = bytesToString( expectedBytes );
            long actualBytes = setting.apply( s -> bytesToString );
            long tenPercent = (long) (expectedBytes * 0.1);
            assertThat( mebibytes + "m",
                    actualBytes,
                    between( expectedBytes - tenPercent, expectedBytes + tenPercent ) );
        }
    }

    private Matcher<Long> between( long lowerBound, long upperBound )
    {
        return both( greaterThanOrEqualTo( lowerBound ) ).and( lessThanOrEqualTo( upperBound ) );
    }

    @Test
    public void mustPrintRecommendationsAsConfigReadableOutput() throws Exception
    {
        StringBuilder output = new StringBuilder();
        Path homeDir = Paths.get( "home" );
        Path configDir = Paths.get( "home", "config" );
        OutsideWorld outsideWorld = new RealOutsideWorld()
        {
            @Override
            public void stdOutLine( String text )
            {
                output.append( text ).append( System.lineSeparator() );
            }
        };
        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand( homeDir, configDir, outsideWorld );
        String heap = bytesToString( recommendHeapMemory( gibiBytes( 8 ) ) );
        String pagecache = bytesToString( recommendPageCacheMemory( gibiBytes( 8 ) ) );

        command.execute( new String[]{"--memory=8g"} );

        Map<String,String> stringMap = MapUtil.load( new StringReader( output.toString() ) );
        assertThat( stringMap.get( initialHeapSize.name() ), is( heap ) );
        assertThat( stringMap.get( maxHeapSize.name() ), is( heap ) );
        assertThat( stringMap.get( pagecache_memory.name() ), is( pagecache ) );
    }

    @Test
    public void shouldPrintKilobytesEvenForByteSizeBelowAKiloByte()
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
        assertThat( stringBelowK, is( "1k" ) );
        assertThat( stringBelow10K, is( "2k" ) );
        assertThat( stringBelow100K, is( "18k" ) );
    }

    @Test
    public void mustPrintMinimalPageCacheMemorySettingForConfiguredDb() throws Exception
    {
        // given
        StringBuilder output = new StringBuilder();
        Path homeDir = directory.directory().toPath();
        Path configDir = homeDir.resolve( "conf" );
        configDir.toFile().mkdirs();
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        String databaseName = "mydb";
        store( stringMap( data_directory.name(), homeDir.toString() ), configFile.toFile() );
        File storeDir = Config.fromFile( configFile ).withHome( homeDir ).withSetting( active_database, databaseName ).build().get( database_path );
        createDatabaseWithNativeIndexes( storeDir );
        OutsideWorld outsideWorld = new RealOutsideWorld()
        {
            @Override
            public void stdOutLine( String text )
            {
                output.append( text ).append( System.lineSeparator() );
            }
        };
        MemoryRecommendationsCommand command = new MemoryRecommendationsCommand( homeDir, configDir, outsideWorld );
        String heap = bytesToString( recommendHeapMemory( gibiBytes( 8 ) ) );
        String pagecache = bytesToString( recommendPageCacheMemory( gibiBytes( 8 ) ) );

        // when
        command.execute( array( "--database", databaseName, "--memory", "8g" ) );

        // then
        String memrecString = output.toString();
        Map<String,String> stringMap = MapUtil.load( new StringReader( memrecString ) );
        assertThat( stringMap.get( initialHeapSize.name() ), is( heap ) );
        assertThat( stringMap.get( maxHeapSize.name() ), is( heap ) );
        assertThat( stringMap.get( pagecache_memory.name() ), is( pagecache ) );

        long[] expectedSizes = calculatePageCacheFileSize( storeDir );
        long expectedPageCacheSize = expectedSizes[0];
        long expectedLuceneSize = expectedSizes[1];
        assertThat( memrecString, containsString( "Lucene indexes: " + bytesToString( expectedLuceneSize ) ) );
        assertThat( memrecString, containsString( "Data volume and native indexes: " + bytesToString( expectedPageCacheSize ) ) );
    }

    private long[] calculatePageCacheFileSize( File storeDir ) throws IOException
    {
        MutableLong pageCacheTotal = new MutableLong();
        MutableLong luceneTotal = new MutableLong();
        for ( StoreType storeType : StoreType.values() )
        {
            if ( storeType.isRecordStore() )
            {
                File file = new File( storeDir, storeType.getStoreFile().storeFileName() );
                long length = file.length();
                pageCacheTotal.add( length );
            }
        }

        Files.walkFileTree( IndexDirectoryStructure.baseSchemaIndexFolder( storeDir ).toPath(), new SimpleFileVisitor<Path>()
        {
            @Override
            public FileVisitResult visitFile( Path path, BasicFileAttributes attrs ) throws IOException
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
        return new long[]{pageCacheTotal.longValue(), luceneTotal.longValue()};
    }

    private void createDatabaseWithNativeIndexes( File storeDir )
    {
        // Create one index for every provider that we have
        for ( GraphDatabaseSettings.SchemaIndex schemaIndex : GraphDatabaseSettings.SchemaIndex.values() )
        {
            GraphDatabaseService db =
                    new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir ).setConfig( default_schema_provider,
                            schemaIndex.providerName() ).newGraphDatabase();
            String key = "key-" + schemaIndex.name();
            try
            {
                try ( Transaction tx = db.beginTx() )
                {
                    db.schema().indexFor( LABEL_ONE ).on( key ).create();
                    tx.success();
                }

                try ( Transaction tx = db.beginTx() )
                {
                    for ( int i = 0; i < 10_000; i++ )
                    {
                        db.createNode( LABEL_ONE ).setProperty( key, randomIndexValue( i ) );
                    }
                    tx.success();
                }
            }
            finally
            {
                db.shutdown();
            }
        }
    }

    private Object randomIndexValue( int i )
    {
        switch ( i % 11 )
        {
        case 0:
            return i;
        case 1:
            return String.valueOf( i );
        case 2:
            return DateValue.now( Clock.systemUTC() );
        case 3:
            return DateTimeValue.now( Clock.systemUTC() );
        case 4:
            return LocalDateTimeValue.now( Clock.systemUTC() );
        case 5:
            return DurationValue.duration( Duration.ofSeconds( 10 ) );
        case 6:
            return TimeValue.now( Clock.systemUTC() );
        case 7:
            return LocalTimeValue.now( Clock.systemUTC() );
        case 8:
            return Values.pointValue( CoordinateReferenceSystem.Cartesian, 1, 2 );
        case 9:
            return Values.pointValue( CoordinateReferenceSystem.Cartesian_3D, 1, 2, 3 );
        case 10:
            return new long[]{i, i + 1, i + 2, i + 3, i + 4, i + 5};
        default:
            throw new UnsupportedOperationException( "Unexpected" );
        }
    }
}
