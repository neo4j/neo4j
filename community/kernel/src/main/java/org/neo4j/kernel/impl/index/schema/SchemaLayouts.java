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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.LayoutBootstrapper;
import org.neo4j.index.internal.gbptree.Meta;
import org.neo4j.index.internal.gbptree.MetadataMismatchException;
import org.neo4j.internal.id.indexed.IdRangeLayout;
import org.neo4j.internal.index.label.LabelScanLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsLayout;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;

public class SchemaLayouts implements LayoutBootstrapper
{
    private final List<LayoutBootstrapper> allSchemaLayout;

    public SchemaLayouts()
    {
        allSchemaLayout = new ArrayList<>();
        allSchemaLayout.addAll( Arrays.asList(
                genericLayout(),
                idRangeLayout(),
                ( indexFile, pageCache, meta ) -> new LabelScanLayout(),
                ( indexFile, pageCache, meta ) -> new IndexStatisticsLayout() ) );
    }

    public static String[] layoutDescriptions()
    {
        return new String[]{
                "Generic layout",
                "Id range layout",
                "Label scan layout",
                "Index statistics layout"
        };
    }

    @Override
    public Layout<?,?> create( File indexFile, PageCache pageCache, Meta meta ) throws IOException
    {
        for ( LayoutBootstrapper factory : allSchemaLayout )
        {
            final Layout<?,?> layout = factory.create( indexFile, pageCache, meta );
            if ( layout != null && matchingLayout( meta, layout ) )
            {
                return layout;
            }
        }
        throw new RuntimeException( "Could not find any layout matching meta " + meta );
    }

    private static boolean matchingLayout( Meta meta, Layout<?,?> layout )
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
        return ( indexFile, pageCache, meta ) ->
        {
            final IndexSpecificSpaceFillingCurveSettings settings = IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
            int maxNumberOfSlots = 10;
            for ( int numberOfSlots = 1; numberOfSlots < maxNumberOfSlots; numberOfSlots++ )
            {
                final GenericLayout genericLayout = new GenericLayout( numberOfSlots, settings );
                if ( matchingLayout( meta, genericLayout ) )
                {
                    return genericLayout;
                }
            }
            return null;
        };
    }

    private static LayoutBootstrapper idRangeLayout()
    {
        return ( indexFile, pageCache, meta ) ->
        {
            int maxExponent = 10;
            for ( int exponent = 0; exponent < maxExponent; exponent++ )
            {
                final int idsPerEntry = 1 << exponent;
                final IdRangeLayout idRangeLayout = new IdRangeLayout( idsPerEntry );
                if ( matchingLayout( meta, idRangeLayout ) )
                {
                    return idRangeLayout;
                }
            }
            return null;
        };
    }
}
