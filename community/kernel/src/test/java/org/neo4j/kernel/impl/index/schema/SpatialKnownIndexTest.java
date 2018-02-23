/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;
import java.nio.file.NoSuchFileException;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptor;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.mockito.Mockito.mock;
import static org.neo4j.kernel.impl.index.schema.fusion.SpatialFusionSchemaIndexProvider.SPATIAL_PROVIDER_DESCRIPTOR;
import static org.neo4j.test.rule.PageCacheRule.config;

public class SpatialKnownIndexTest
{
    private final FileSystemRule fsRule = new EphemeralFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fsRule.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );
    protected final RandomRule random = new RandomRule();
    @Rule
    public final RuleChain rules = outerRule( fsRule ).around( directory ).around( pageCacheRule ).around( random );

    private SpatialCRSSchemaIndex index;
    private IndexDescriptor descriptor;
    private IndexSamplingConfig samplingConfig;
    private FileSystemAbstraction fs;
    private File storeDir;
    private File indexDir;

    @Before
    public void setup() throws IOException
    {
        fs = fsRule.get();
        storeDir = new File( "store" );
        fs.deleteRecursively( storeDir );
        fs.mkdir( storeDir );

        CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
        String crsDir = String.format("%s-%s", crs.getTable().getTableId(), crs.getCode() );
        indexDir = new File( new File( new File( new File( new File( storeDir, "schema" ), "index" ), "spatial-1.0" ), "1" ), crsDir );
        IndexDirectoryStructure dirStructure = IndexDirectoryStructure.directoriesByProvider( storeDir ).forProvider( SPATIAL_PROVIDER_DESCRIPTOR );
        descriptor = IndexDescriptorFactory.forLabel( 42, 1337 );
        index = new SpatialCRSSchemaIndex( descriptor, dirStructure, crs, 1L, pageCacheRule.getPageCache( fs ), fs,
                SchemaIndexProvider.Monitor.EMPTY, RecoveryCleanupWorkCollector.IMMEDIATE );
        samplingConfig = mock( IndexSamplingConfig.class );
    }

    @Test
    public void shouldCreateFileOnCreate() throws IOException
    {
        // given
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );

        // when
        index.startPopulation();

        // then
        assertThat( fs.listFiles( indexDir ).length, equalTo( 1 ) );
        index.finishPopulation( true );
    }

    @Test
    public void shouldNotTakeOnlineIfIndexNotCreated() throws IOException
    {
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );

        try
        {
            // when
            index.takeOnline();
            fail("should have thrown exception");
        }
        catch ( IOException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Index file does not exist." ) );
            assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );
        }
    }

    @Test
    public void shouldNotTakeOnlineIfPopulating() throws IOException
    {
        // given
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );
        index.startPopulation();

        // when
        try
        {
            index.takeOnline();
            fail( "Should have thrown exception." );
        }
        catch ( IllegalStateException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Failed to bring index online." ) );
            index.finishPopulation( true );
        }
    }

    @Test
    public void shouldTakeOnlineIfDonePopulating() throws IOException
    {
        // given
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );
        index.startPopulation();
        index.finishPopulation( true );

        // when
        index.takeOnline();
        index.close();
    }

    @Test
    public void shouldGetUpdaterWhenOnline() throws IOException, IndexEntryConflictException
    {
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );

        // when
        IndexUpdater updater = index.updaterWithCreate( false );

        // then
        assertThat( fs.listFiles( indexDir ).length, equalTo( 1 ) );

        updater.close();
        index.close();
    }

    @Test
    public void shouldGetUpdaterWhenPopulating() throws IOException, IndexEntryConflictException
    {
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );

        // when
        IndexUpdater updater = index.updaterWithCreate( true );

        // then
        assertThat( fs.listFiles( indexDir ).length, equalTo( 1 ) );

        updater.close();
        index.finishPopulation( true );
    }

    @Test
    public void drop() throws IOException
    {
        // given
        assertThat( fs.listFiles( storeDir ).length, equalTo( 0 ) );
        index.startPopulation();
        assertThat( fs.listFiles( indexDir ).length, equalTo( 1 ) );

        // when
        index.drop();

        // then
        assertThat( fs.listFiles( indexDir ).length, equalTo( 0 ) );
    }

    @Test
    public void shouldThrowIfFileNotExistReadingInitialState()
    {
        try
        {
            index.readState( descriptor );
            fail( "Should throw if no index file exists." );
        }
        catch ( IOException e )
        {
            assertTrue( e instanceof NoSuchFileException );
        }
    }
}
