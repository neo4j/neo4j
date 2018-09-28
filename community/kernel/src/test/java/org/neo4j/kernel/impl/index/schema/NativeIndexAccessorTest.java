/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.ValueGroup;

@RunWith( Parameterized.class )
public class NativeIndexAccessorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexAccessorTests<KEY,VALUE>
{
    @Parameterized.Parameters( name = "{index}: {0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {"Number",
                        numberAccessorFactory(),
                        (LayoutTestUtilFactory) NumberLayoutTestUtil::new
                },
                {"String",
                        stringAccessorFactory(),
                        (LayoutTestUtilFactory) StringLayoutTestUtil::new
                },
                {"Date",
                        temporalAccessorFactory( ValueGroup.DATE ),
                        (LayoutTestUtilFactory) DateLayoutTestUtil::new
                },
                {"DateTime",
                        temporalAccessorFactory( ValueGroup.ZONED_DATE_TIME ),
                        (LayoutTestUtilFactory) DateTimeLayoutTestUtil::new
                },
                {"Duration",
                        temporalAccessorFactory( ValueGroup.DURATION ),
                        (LayoutTestUtilFactory) DurationLayoutTestUtil::new
                },
                {"LocalDateTime",
                        temporalAccessorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        (LayoutTestUtilFactory) LocalDateTimeLayoutTestUtil::new
                },
                {"LocalTime",
                        temporalAccessorFactory( ValueGroup.LOCAL_TIME ),
                        (LayoutTestUtilFactory) LocalTimeLayoutTestUtil::new
                },
                {"LocalDateTime",
                        temporalAccessorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        (LayoutTestUtilFactory) LocalDateTimeLayoutTestUtil::new
                },
                {"Time",
                        temporalAccessorFactory( ValueGroup.ZONED_TIME ),
                        (LayoutTestUtilFactory) TimeLayoutTestUtil::new
                },
                {"Generic",
                        genericAccessorFactory(),
                        (LayoutTestUtilFactory) GenericLayoutTestUtil::new
                },
                //{ Spatial has it's own subclass because it need to override some of the test methods }
        } );
    }

    private final AccessorFactory<KEY,VALUE> accessorFactory;
    private final LayoutTestUtilFactory<KEY,VALUE> layoutTestUtilFactory;

    @SuppressWarnings( "unused" )
    public NativeIndexAccessorTest( String name, AccessorFactory<KEY,VALUE> accessorFactory, LayoutTestUtilFactory<KEY,VALUE> layoutTestUtilFactory )
    {
        this.accessorFactory = accessorFactory;
        this.layoutTestUtilFactory = layoutTestUtilFactory;
    }

    @Override
    NativeIndexAccessor<KEY,VALUE> makeAccessor() throws IOException
    {
        return accessorFactory.create( pageCache, fs, getIndexFile(), layout, RecoveryCleanupWorkCollector.immediate(), monitor, indexDescriptor );
    }

    @Override
    LayoutTestUtil<KEY,VALUE> createLayoutTestUtil()
    {
        return layoutTestUtilFactory.create( TestIndexDescriptorFactory.forLabel( 42, 666 ).withId( 0 ) );
    }

    /* Helpers */
    private static AccessorFactory<NumberIndexKey,NativeIndexValue> numberAccessorFactory()
    {
        return NumberIndexAccessor::new;
    }

    private static AccessorFactory<StringIndexKey,NativeIndexValue> stringAccessorFactory()
    {
        return StringIndexAccessor::new;
    }

    private static <TK extends NativeIndexSingleValueKey<TK>> AccessorFactory<TK,NativeIndexValue> temporalAccessorFactory( ValueGroup temporalValueGroup )
    {
        return ( pageCache, fs, storeFile, layout, cleanup, monitor, descriptor ) ->
        {
            TemporalIndexFiles.FileLayout<TK> fileLayout = new TemporalIndexFiles.FileLayout<>( storeFile, layout, temporalValueGroup );
            return new TemporalIndexAccessor.PartAccessor<>( pageCache, fs, fileLayout, cleanup, monitor, descriptor );
        };
    }

    private static AccessorFactory<CompositeGenericKey,NativeIndexValue> genericAccessorFactory()
    {
        IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings =
                new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );
        StandardConfiguration configuration = new StandardConfiguration();
        return ( pageCache, fs, storeFile, layout, cleanup, monitor, descriptor ) ->
                new GenericNativeIndexAccessor( pageCache, fs, storeFile, layout, cleanup, monitor, descriptor, spaceFillingCurveSettings, configuration );
    }

    @FunctionalInterface
    private interface AccessorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexAccessor<KEY,VALUE> create( PageCache pageCache, FileSystemAbstraction fs,
                File storeFile, IndexLayout<KEY,VALUE> layout, RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
                IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor ) throws IOException;
    }
}
