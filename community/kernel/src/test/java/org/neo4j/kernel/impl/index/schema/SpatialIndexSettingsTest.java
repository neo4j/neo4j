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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.io.IOException;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptor;
import org.neo4j.kernel.api.schema.index.SchemaIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.sampling.IndexSamplingConfig;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.kernel.impl.index.schema.config.SpatialIndexSettings;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.kernel.api.index.IndexDirectoryStructure.directoriesByProvider;
import static org.neo4j.kernel.impl.api.index.IndexUpdateMode.ONLINE;
import static org.neo4j.test.rule.PageCacheRule.config;

public class SpatialIndexSettingsTest
{
    private static final CoordinateReferenceSystem crs = CoordinateReferenceSystem.WGS84;
    private static final Config config1 = Config.defaults();
    private static final Config config2 = configWithRange( 0, -90, 180, 90 );
    private static final SpaceFillingCurveSettingsFactory settings1 = new SpaceFillingCurveSettingsFactory( config1 );
    private static final SpaceFillingCurveSettingsFactory settings2 = new SpaceFillingCurveSettingsFactory( config2 );

    private SchemaIndexDescriptor schemaIndexDescriptor1;
    private SchemaIndexDescriptor schemaIndexDescriptor2;
    private LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue> layoutUtil1;
    private LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue> layoutUtil2;
    private long indexId1 = 1;
    private long indexId2 = 2;

    final DefaultFileSystemRule fs = new DefaultFileSystemRule();
    private final TestDirectory directory = TestDirectory.testDirectory( getClass(), fs.get() );
    private final PageCacheRule pageCacheRule = new PageCacheRule( config().withAccessChecks( true ) );

    @Rule
    public final RuleChain rules = outerRule( fs ).around( directory ).around( pageCacheRule );

    private PageCache pageCache;
    private IndexProvider.Monitor monitor = IndexProvider.Monitor.EMPTY;

    @Before
    public void setupTwoIndexes() throws IOException
    {
        pageCache = pageCacheRule.getPageCache( fs );

        // Define two indexes based on different labels and different settings
        layoutUtil1 = createLayoutTestUtil( 42, settings1 );
        layoutUtil2 = createLayoutTestUtil( 43, settings2 );
        schemaIndexDescriptor1 = layoutUtil1.indexDescriptor();
        schemaIndexDescriptor2 = layoutUtil2.indexDescriptor();

        // Create the two indexes as empty, based on differently configured settings above
        createEmptyIndex( indexId1, schemaIndexDescriptor1, settings1 );
        createEmptyIndex( indexId2, schemaIndexDescriptor2, settings2 );
    }

    @Test
    public void shouldAddToSpatialIndexWithDefaults() throws Exception
    {
        // given
        SpatialIndexProvider provider = newSpatialIndexProvider( config1 );
        addUpdates( provider, indexId1, schemaIndexDescriptor1, layoutUtil1 );

        // then
        verifySpatialSettings( indexFile( indexId1 ), settings1.settingsFor( crs ) );
    }

    @Test
    public void shouldAddToSpatialIndexWithModifiedSettings() throws Exception
    {
        // given
        SpatialIndexProvider provider = newSpatialIndexProvider( config2 );
        addUpdates( provider, indexId2, schemaIndexDescriptor2, layoutUtil2 );

        // then
        verifySpatialSettings( indexFile( indexId2 ), settings2.settingsFor( crs ) );
    }

    @Test
    public void shouldAddToTwoDifferentIndexesOneDefaultAndOneModified() throws Exception
    {
        // given
        SpatialIndexProvider provider = newSpatialIndexProvider( config2 );
        addUpdates( provider, indexId1, schemaIndexDescriptor1, layoutUtil1 );
        addUpdates( provider, indexId2, schemaIndexDescriptor2, layoutUtil2 );

        // then even though the provider was created with modified settings, only the second index should have them
        verifySpatialSettings( indexFile( indexId1 ), settings1.settingsFor( crs ) );
        verifySpatialSettings( indexFile( indexId2 ), settings2.settingsFor( crs ) );
    }

    @Test
    public void shouldNotLeakSpaceFillingCurveSettingsBetweenExistingAndNewIndexes() throws Exception
    {
        // given two indexes previously created with different settings
        Config config = configWithRange( -10, -10, 10, 10 );
        SpatialIndexProvider provider = newSpatialIndexProvider( config );
        addUpdates( provider, indexId1, schemaIndexDescriptor1, layoutUtil1 );
        addUpdates( provider, indexId2, schemaIndexDescriptor2, layoutUtil2 );

        // and when creating and populating a third index with a third set of settings
        long indexId3 = 3;
        SpaceFillingCurveSettingsFactory settings3 = new SpaceFillingCurveSettingsFactory( config );
        SpatialLayoutTestUtil layoutUtil3 = createLayoutTestUtil( 44, settings3 );
        SchemaIndexDescriptor schemaIndexDescriptor3 = layoutUtil3.indexDescriptor();
        createEmptyIndex( indexId3, schemaIndexDescriptor3, provider );
        addUpdates( provider, indexId3, schemaIndexDescriptor3, layoutUtil3 );

        // Then all indexes should still have their own correct and different settings
        verifySpatialSettings( indexFile( indexId1 ), settings1.settingsFor( crs ) );
        verifySpatialSettings( indexFile( indexId2 ), settings2.settingsFor( crs ) );
        verifySpatialSettings( indexFile( indexId3 ), settings3.settingsFor( crs ) );
    }

    private IndexSamplingConfig samplingConfig()
    {
        return new IndexSamplingConfig( Config.defaults() );
    }

    private SpatialLayoutTestUtil createLayoutTestUtil( int labelId, SpaceFillingCurveSettingsFactory settings )
    {
        return new SpatialLayoutTestUtil( SchemaIndexDescriptorFactory.forLabel( labelId, 666 ), settings.settingsFor( crs ), crs );
    }

    private SpatialIndexProvider newSpatialIndexProvider( Config config )
    {
        return new SpatialIndexProvider( pageCache, fs, directoriesByProvider( directory.graphDbDir() ), monitor, immediate(), false, config );
    }

    private void addUpdates( SpatialIndexProvider provider, long indexId, SchemaIndexDescriptor schemaIndexDescriptor,
            LayoutTestUtil<SpatialSchemaKey,NativeSchemaValue> layoutUtil ) throws IOException, IndexEntryConflictException
    {
        IndexAccessor accessor = provider.getOnlineAccessor( indexId, schemaIndexDescriptor, samplingConfig() );
        try ( IndexUpdater updater = accessor.newUpdater( ONLINE ) )
        {
            // when
            for ( IndexEntryUpdate<SchemaIndexDescriptor> update : layoutUtil.someUpdates() )
            {
                updater.process( update );
            }
        }
        accessor.force( IOLimiter.unlimited() );
        accessor.close();
    }

    private SpatialIndexFiles.SpatialFile makeIndexFile( long indexId, SpaceFillingCurveSettingsFactory settings )
    {
        return new SpatialIndexFiles.SpatialFile( CoordinateReferenceSystem.WGS84, settings, indexDir( indexId ) );
    }

    private File indexDir( long indexId )
    {
        return new File( indexRoot(), Long.toString( indexId ) );
    }

    private File indexFile( long indexId )
    {
        // The indexFile location is independent of the settings, so we just use the defaults
        return makeIndexFile( indexId, new SpaceFillingCurveSettingsFactory( Config.defaults() ) ).indexFile;
    }

    private File indexRoot()
    {
        return new File( new File( new File( directory.graphDbDir(), "schema" ), "index" ), "spatial-1.0" );
    }

    private void createEmptyIndex( long indexId, SchemaIndexDescriptor schemaIndexDescriptor, SpaceFillingCurveSettingsFactory settings ) throws IOException
    {
        SpatialIndexFiles.SpatialFileLayout fileLayout = makeIndexFile( indexId, settings ).getLayoutForNewIndex();
        SpatialIndexPopulator.PartPopulator populator =
                new SpatialIndexPopulator.PartPopulator( pageCache, fs, fileLayout, monitor, schemaIndexDescriptor, indexId, samplingConfig(),
                        new StandardConfiguration() );
        populator.create();
        populator.close( true );
    }

    private void createEmptyIndex( long indexId, SchemaIndexDescriptor schemaIndexDescriptor, SpatialIndexProvider provider ) throws IOException
    {
        IndexPopulator populator = provider.getPopulator( indexId, schemaIndexDescriptor, samplingConfig() );
        populator.create();
        populator.close( true );
    }

    private void verifySpatialSettings( File indexFile, SpaceFillingCurveSettings expectedSettings )
    {
        try
        {
            SpaceFillingCurveSettings settings =
                    SpaceFillingCurveSettings.fromGBPTree( indexFile, pageCache, NativeSchemaIndexHeaderReader::readFailureMessage );
            assertThat( "Should get correct results from header", settings, equalTo( expectedSettings ) );
        }
        catch ( IOException e )
        {
            fail( "Failed to read GBPTree header: " + e.getMessage() );
        }
    }

    private static Config configWithRange( double minX, double minY, double maxX, double maxY )
    {
        Setting<Double> wgs84MinX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 0, "min" );
        Setting<Double> wgs84MinY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 1, "min" );
        Setting<Double> wgs84MaxX = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 0, "max" );
        Setting<Double> wgs84MaxY = SpatialIndexSettings.makeCRSRangeSetting( CoordinateReferenceSystem.WGS84, 1, "max" );
        Config config = Config.defaults();
        config.augment( wgs84MinX, Double.toString( minX ) );
        config.augment( wgs84MinY, Double.toString( minY ) );
        config.augment( wgs84MaxX, Double.toString( maxX ) );
        config.augment( wgs84MaxY, Double.toString( maxY ) );
        return config;
    }
}
