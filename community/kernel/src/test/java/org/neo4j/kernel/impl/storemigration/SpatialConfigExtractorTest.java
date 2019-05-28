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
package org.neo4j.kernel.impl.storemigration;

import org.eclipse.collections.api.tuple.Pair;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.schema.IndexConfig;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.test.Unzip.unzip;
import static org.neo4j.values.storable.Values.COMPARATOR;

@PageCacheExtension
class SpatialConfigExtractorTest
{
    private static final String ZIP_HEALTHY_SPATIAL_35_DIR = "healthy-spatial-35-dir.zip";
    private static final String HEALTHY_SPATIAL_35_DIR = "healthy-spatial-35-dir";
    private static final String ZIP_FAILED_SPATIAL_35_DIR = "failed-spatial-35-dir.zip";
    private static final String FAILED_SPATIAL_35_DIR = "failed-spatial-35-dir";
    private static final Map<String,Value> staticExpectedIndexConfig = new HashMap<>();

    static
    {
        staticExpectedIndexConfig.put( "spatial.wgs-84.tableId", Values.intValue( 1 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.code", Values.intValue( 4326 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.dimensions", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.maxLevels", Values.intValue( 15 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.min", Values.doubleArray( new double[]{-1.0, -2.0} ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84.max", Values.doubleArray( new double[]{3.0, 4.0} ) );

        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.tableId", Values.intValue( 1 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.code", Values.intValue( 4979 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.dimensions", Values.intValue( 3 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.maxLevels", Values.intValue( 10 ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.min", Values.doubleArray( new double[]{-5.0, -6.0, -7.0} ) );
        staticExpectedIndexConfig.put( "spatial.wgs-84-3d.max", Values.doubleArray( new double[]{8.0, 9.0, 10.0} ) );

        staticExpectedIndexConfig.put( "spatial.cartesian.tableId", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.code", Values.intValue( 7203 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.dimensions", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.maxLevels", Values.intValue( 15 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.min", Values.doubleArray( new double[]{-11.0, -12.0} ) );
        staticExpectedIndexConfig.put( "spatial.cartesian.max", Values.doubleArray( new double[]{13.0, 14.0} ) );

        staticExpectedIndexConfig.put( "spatial.cartesian-3d.tableId", Values.intValue( 2 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.code", Values.intValue( 9157 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.dimensions", Values.intValue( 3 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.maxLevels", Values.intValue( 10 ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.min", Values.doubleArray( new double[]{-15.0, -16.0, -17.0} ) );
        staticExpectedIndexConfig.put( "spatial.cartesian-3d.max", Values.doubleArray( new double[]{18.0, 19.0, 20.0} ) );
    }

    @Inject
    FileSystemAbstraction fs;

    @Inject
    PageCache pageCache;

    @Inject
    TestDirectory directory;

    @Test
    void shouldLogFailureToExtractIndexConfigFromGenericBecauseOfIndexInFailedState() throws IOException
    {
        // given
        unzip( getClass(), ZIP_FAILED_SPATIAL_35_DIR, directory.directory() );
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log myLog = logProvider.getLog( "myLog" );

        // and
        File spatialDirectory = new File( directory.directory(), FAILED_SPATIAL_35_DIR );
        assertTrue( fs.fileExists( spatialDirectory ) );
        assertTrue( fs.isDirectory( spatialDirectory ) );

        // when
        List<SpatialFile> spatialFiles = IndexMigration.getSpatialFiles( fs, spatialDirectory );
        SpatialConfigExtractor.indexConfigFromSpatialFile( pageCache, spatialFiles, myLog );

        // then
        String reason = "Index is in FAILED state.";
        AssertableLogProvider.LogMatcher logEntry = getExpectedLogEntry( spatialFiles.get( 0 ).getIndexFile(), reason );
        logProvider.assertExactly( logEntry );
    }

    @Test
    void shouldLogFailureToExtractIndexConfigFromGenericBecauseOfIndexInCorruptState() throws IOException
    {
        // given
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log myLog = logProvider.getLog( "myLog" );
        SpatialFile spatialFile = new SpatialFile( CoordinateReferenceSystem.WGS84, directory.file( "spatialFile" ) );
        corruptFile( fs, spatialFile.getIndexFile() );

        // when
        SpatialConfigExtractor.indexConfigFromSpatialFile( pageCache, singletonList( spatialFile ), myLog );

        // then
        String reason = "Index meta data is corrupt and can not be parsed.";
        AssertableLogProvider.LogMatcher logEntry = getExpectedLogEntry( spatialFile.getIndexFile(), reason );
        logProvider.assertExactly( logEntry );
    }

    @Test
    void shouldBeAbleToExtractConfigFromHealthy35File() throws IOException
    {
        // given
        unzip( getClass(), ZIP_HEALTHY_SPATIAL_35_DIR, directory.directory() );
        File spatialDir = directory.file( HEALTHY_SPATIAL_35_DIR );

        // and
        assertTrue( fs.fileExists( spatialDir ) );

        // when
        List<SpatialFile> spatialFiles = IndexMigration.getSpatialFiles( fs, spatialDir );
        IndexConfig indexConfig = SpatialConfigExtractor.indexConfigFromSpatialFile( pageCache, spatialFiles, NullLog.getInstance() );

        // then
        assertExpectedIndexConfig( indexConfig );
    }

    private static AssertableLogProvider.LogMatcher getExpectedLogEntry( File spatialFile, String reason )
    {
        return AssertableLogProvider.inLog( "myLog" )
                .warn( Matchers.allOf(
                        Matchers.containsString( "Could not extract index configuration from migrating index file." ),
                        Matchers.containsString( reason ),
                        Matchers.containsString(
                                "Index will be recreated with currently configured settings instead, indexFile=" + spatialFile.getAbsolutePath() )
                ) );
    }

    private static void corruptFile( FileSystemAbstraction fs, File spatialFile ) throws IOException
    {
        try ( StoreChannel write = fs.write( spatialFile ) )
        {
            int size = 100;
            byte[] bytes = new byte[size];
            Arrays.fill( bytes, (byte) 9 );
            ByteBuffer byteBuffer = ByteBuffer.allocate( size );
            byteBuffer.put( bytes );
            write.writeAll( byteBuffer );
        }
        assertTrue( fs.fileExists( spatialFile ) );
    }

    private static void assertExpectedIndexConfig( IndexConfig indexConfig )
    {
        Map<String,Value> expectedIndexConfig = new HashMap<>( staticExpectedIndexConfig );
        for ( Pair<String,Value> entry : indexConfig.entries() )
        {
            String actualKey = entry.getOne();
            Value actualValue = entry.getTwo();
            Value expectedValue = expectedIndexConfig.remove( actualKey );
            assertNotNull( expectedValue, "Actual index config had map entry that was not among expected " + entry );
            assertEquals( 0, COMPARATOR.compare( expectedValue, actualValue ),
                    String.format( "Expected and actual index config value differed for %s, expected %s but was %s.", actualKey, expectedValue,
                            actualValue ) );
        }
        assertTrue( expectedIndexConfig.isEmpty(), "Actual index config was missing some values: " + expectedIndexConfig );
    }
}
