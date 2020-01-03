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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.InternalIndexState;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.neo4j.internal.kernel.api.InternalIndexState.ONLINE;
import static org.neo4j.internal.kernel.api.InternalIndexState.POPULATING;

@RunWith( Parameterized.class )
public class NativeIndexProviderTest extends NativeIndexProviderTests
{
    @Parameterized.Parameters( name = "{index} {0}" )
    public static Object[][] data()
    {
        return new Object[][]{
                {"Number",
                        (ProviderFactory) NumberIndexProvider::new,
                        POPULATING,
                        Values.of( 1 )
                },
                {"String",
                        (ProviderFactory) StringIndexProvider::new,
                        POPULATING,
                        Values.of( "string" )
                },
                {"Spatial",
                        spatialProviderFactory(),
                        ONLINE,
                        Values.pointValue( CoordinateReferenceSystem.WGS84, 0, 0 )
                },
                {"Temporal",
                        (ProviderFactory) TemporalIndexProvider::new,
                        ONLINE,
                        DateValue.date( 1, 1, 1 )
                },
                {"Generic",
                        genericProviderFactory(),
                        POPULATING,
                        Values.of( 1 )
                },
        };
    }

    private static ProviderFactory genericProviderFactory()
    {
        return ( pageCache, fs, dir, monitor, collector, readOnly ) ->
                new GenericNativeIndexProvider( dir, pageCache, fs, monitor, collector, readOnly, Config.defaults() );
    }

    private static ProviderFactory spatialProviderFactory()
    {
        return ( pageCache, fs, dir, monitor, collector, readOnly ) ->
                new SpatialIndexProvider( pageCache, fs, dir, monitor, collector, readOnly, Config.defaults() );
    }

    @Parameterized.Parameter
    public String name;

    @Parameterized.Parameter( 1 )
    public ProviderFactory providerFactory;

    @Parameterized.Parameter( 2 )
    public InternalIndexState expectedStateOnNonExistingSubIndex;

    @Parameterized.Parameter( 3 )
    public Value someValue;

    @Override
    protected InternalIndexState expectedStateOnNonExistingSubIndex()
    {
        return expectedStateOnNonExistingSubIndex;
    }

    @Override
    protected Value someValue()
    {
        return someValue;
    }

    @Override
    IndexProvider newProvider( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir, IndexProvider.Monitor monitor,
            RecoveryCleanupWorkCollector collector, boolean readOnly )
    {
        return providerFactory.create( pageCache, fs, dir, monitor, collector, readOnly );
    }

    @FunctionalInterface
    private interface ProviderFactory
    {
        IndexProvider create( PageCache pageCache, FileSystemAbstraction fs, IndexDirectoryStructure.Factory dir, IndexProvider.Monitor monitor,
                RecoveryCleanupWorkCollector collector, boolean readOnly );
    }
}
