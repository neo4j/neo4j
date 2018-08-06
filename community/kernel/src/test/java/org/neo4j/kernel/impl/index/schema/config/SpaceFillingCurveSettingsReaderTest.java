package org.neo4j.kernel.impl.index.schema.config;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.io.pagecache.ByteArrayPageCursor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D;

public class SpaceFillingCurveSettingsReaderTest
{
    @Test
    public void shouldReadMultipleSettings()
    {
        // given
        ConfiguredSpaceFillingCurveSettingsCache globalSettings = new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );
        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> expectedSpecificSettings = new HashMap<>();
        rememberSettings( globalSettings, expectedSpecificSettings, WGS84, WGS84_3D, Cartesian );
        IndexSpecificSpaceFillingCurveSettingsCache specificSettings =
                new IndexSpecificSpaceFillingCurveSettingsCache( globalSettings, expectedSpecificSettings );
        SpaceFillingCurveSettingsWriter writer = new SpaceFillingCurveSettingsWriter( specificSettings );
        byte[] bytes = new byte[PageCache.PAGE_SIZE];
        writer.accept( new ByteArrayPageCursor( bytes ) );

        Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> readExpectedSettings = new HashMap<>();
        SpaceFillingCurveSettingsReader reader = new SpaceFillingCurveSettingsReader( readExpectedSettings );

        // when
        reader.read( ByteBuffer.wrap( bytes ) );

        // then
        assertEquals( expectedSpecificSettings, readExpectedSettings );
    }

    private void rememberSettings( ConfiguredSpaceFillingCurveSettingsCache globalSettings,
            Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> expectedSpecificSettings, CoordinateReferenceSystem... crss )
    {
        for ( CoordinateReferenceSystem crs : crss )
        {
            expectedSpecificSettings.put( crs, globalSettings.forCRS( crs ) );
        }
    }
}
