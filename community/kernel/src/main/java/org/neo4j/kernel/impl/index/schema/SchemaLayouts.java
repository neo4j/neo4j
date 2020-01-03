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
package org.neo4j.kernel.impl.index.schema;

import org.apache.commons.lang3.mutable.MutableBoolean;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.LayoutBootstrapper;
import org.neo4j.index.internal.gbptree.Meta;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.labelscan.LabelScanLayout;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettings;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsFactory;
import org.neo4j.kernel.impl.index.schema.config.SpaceFillingCurveSettingsReader;
import org.neo4j.values.storable.CoordinateReferenceSystem;

public class SchemaLayouts implements LayoutBootstrapper
{
    private final List<LayoutBootstrapper> allSchemaLayout;

    public SchemaLayouts()
    {
        allSchemaLayout = new ArrayList<>();
        allSchemaLayout.addAll( Arrays.asList(
                spatialLayoutFactory( CoordinateReferenceSystem.WGS84 ),
                spatialLayoutFactory( CoordinateReferenceSystem.WGS84_3D ),
                spatialLayoutFactory( CoordinateReferenceSystem.Cartesian ),
                spatialLayoutFactory( CoordinateReferenceSystem.Cartesian_3D ),
                ( indexFile, pageCache, meta, targetLayout ) -> new LocalTimeLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new ZonedDateTimeLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new DurationLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new ZonedTimeLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new DateLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new LocalDateTimeLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new StringLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new NumberLayoutUnique(),
                ( indexFile, pageCache, meta, targetLayout ) -> new NumberLayoutNonUnique(),
                genericLayout(),
                ( indexFile, pageCache, meta, targetLayout ) -> new LabelScanLayout() ) );
    }

    public static String[] layoutDescriptions()
    {
        return new String[]{
                "Generic layout with given number of slots - 'generic1', 'generic2', etc",
                "Spatial cartesian - 'cartesian'",
                "Spatial cartesian 3d - 'cartesian-3d'",
                "Spatial wgs-84 (geographic) - 'wgs-84'",
                "Spatial wgs-84-3d (geographic) - 'wgs-84-3d'",
                "Local time - No user input needed",
                "Zoned date time - No user input needed",
                "Duration - No user input needed",
                "Zoned time - No user input needed",
                "Date - No user input needed",
                "Local date time - No user input needed",
                "String - No user input needed",
                "Number unique - No user input needed",
                "Number non-unique - No user input needed",
                "Label scan - No user input needed"
        };
    }

    @Override
    public Layout<?,?> create( File indexFile, PageCache pageCache, Meta meta, String targetLayout ) throws IOException
    {
        for ( LayoutBootstrapper factory : allSchemaLayout )
        {
            final Layout<?,?> layout = factory.create( indexFile, pageCache, meta, targetLayout );
            if ( layout != null && matchingLayout( meta, layout ) )
            {
                // Verify spatial and generic
                return layout;
            }
        }
        throw new RuntimeException( "Layout with identifier \"" + targetLayout + "\" did not match meta " + meta );
    }

    private static boolean matchingLayout( Meta meta, Layout layout )
    {
        try
        {
            meta.verify( layout );
            return true;
        }
        catch ( MetadataMismatchException e )
        {
            return false;
        }
    }

    private static LayoutBootstrapper genericLayout()
    {
        return ( indexFile, pageCache, meta, targetLayout ) ->
        {
            if ( targetLayout != null && targetLayout.contains( "generic" ) )
            {
                final String numberOfSlotsString = targetLayout.replace( "generic", "" );
                final int numberOfSlots = Integer.parseInt( numberOfSlotsString );
                final Map<CoordinateReferenceSystem,SpaceFillingCurveSettings> settings = new HashMap<>();
                GBPTree.readHeader( pageCache, indexFile, new NativeIndexHeaderReader( new SpaceFillingCurveSettingsReader( settings ) ) );
                final ConfiguredSpaceFillingCurveSettingsCache configuredSettings =
                        new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() );
                return new GenericLayout( numberOfSlots, new IndexSpecificSpaceFillingCurveSettingsCache( configuredSettings, settings ) );
            }
            return null;
        };
    }

    private static LayoutBootstrapper spatialLayoutFactory( CoordinateReferenceSystem crs )
    {
        return ( indexFile, pageCache, meta, targetLayout ) -> {
            if ( crs.getName().equals( targetLayout ) )
            {
                final MutableBoolean failure = new MutableBoolean( false );
                final Function<ByteBuffer,String> onError = byteBuffer ->
                {
                    failure.setTrue();
                    return "";
                };
                final SpaceFillingCurveSettings curveSettings = SpaceFillingCurveSettingsFactory.fromGBPTree( indexFile, pageCache, onError );
                if ( !failure.getValue() )
                {
                    return new SpatialLayout( crs, curveSettings.curve() );
                }
            }
            return null;
        };
    }
}
