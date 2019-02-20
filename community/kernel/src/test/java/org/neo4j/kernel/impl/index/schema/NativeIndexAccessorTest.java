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

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.IndexCapability;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;

import static org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory.forLabel;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;

@RunWith( Parameterized.class )
public class NativeIndexAccessorTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexAccessorTests<KEY,VALUE>
{
    @Parameterized.Parameters( name = "{index}: {0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {"Number",
                        numberAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.NUMBER ),
                        (IndexLayoutFactory) NumberLayoutNonUnique::new,
                        NumberIndexProvider.CAPABILITY
                },
                {"String",
                        stringAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.TEXT ),
                        (IndexLayoutFactory) StringLayout::new,
                        StringIndexProvider.CAPABILITY
                },
                {"Date",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.DATE ),
                        (IndexLayoutFactory) DateLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"DateTime",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.ZONED_DATE_TIME ),
                        (IndexLayoutFactory) ZonedDateTimeLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"Duration",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.DURATION ),
                        (IndexLayoutFactory) DurationLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"LocalDateTime",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_DATE_TIME ),
                        (IndexLayoutFactory) LocalDateTimeLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"LocalTime",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_TIME ),
                        (IndexLayoutFactory) LocalTimeLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"LocalDateTime",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_DATE_TIME ),
                        (IndexLayoutFactory) LocalDateTimeLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"Time",
                        temporalAccessorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.ZONED_TIME ),
                        (IndexLayoutFactory) ZonedTimeLayout::new,
                        TemporalIndexProvider.CAPABILITY
                },
                {"Generic",
                        genericAccessorFactory(),
                        ValueType.values(),
                        (IndexLayoutFactory) () -> new GenericLayout( 1, spaceFillingCurveSettings ),
                        GenericNativeIndexProvider.CAPABILITY
                },
                //{ Spatial has it's own subclass because it need to override some of the test methods }
        } );
    }

    private static final IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), Collections.emptyMap() );
    private static final StandardConfiguration configuration = new StandardConfiguration();

    private final AccessorFactory<KEY,VALUE> accessorFactory;
    private final ValueType[] supportedTypes;
    private final IndexLayoutFactory<KEY,VALUE> indexLayoutFactory;
    private final IndexCapability indexCapability;

    @SuppressWarnings( "unused" )
    public NativeIndexAccessorTest( String name,
            AccessorFactory<KEY,VALUE> accessorFactory,
            ValueType[] supportedTypes,
            IndexLayoutFactory<KEY,VALUE> indexLayoutFactory,
            IndexCapability indexCapability )
    {
        this.accessorFactory = accessorFactory;
        this.supportedTypes = supportedTypes;
        this.indexLayoutFactory = indexLayoutFactory;
        this.indexCapability = indexCapability;
    }

    @Override
    NativeIndexAccessor<KEY,VALUE> makeAccessor() throws IOException
    {
        return accessorFactory.create( pageCache, fs, indexFiles, layout, RecoveryCleanupWorkCollector.immediate(), monitor, indexDescriptor );
    }

    @Override
    IndexCapability indexCapability()
    {
        return indexCapability;
    }

    @Override
    ValueCreatorUtil<KEY,VALUE> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( forLabel( 42, 666 ).withId( 0 ), supportedTypes, FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexLayout<KEY,VALUE> createLayout()
    {
        return indexLayoutFactory.create();
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

    private static <TK extends NativeIndexSingleValueKey<TK>> AccessorFactory<TK,NativeIndexValue> temporalAccessorFactory()
    {
        return TemporalIndexAccessor.PartAccessor::new;
    }

    private static AccessorFactory<GenericKey,NativeIndexValue> genericAccessorFactory()
    {
        return ( pageCache, fs, storeFiles, layout, cleanup, monitor, descriptor ) ->
                new GenericNativeIndexAccessor( pageCache, fs, storeFiles, layout, cleanup, monitor, descriptor, spaceFillingCurveSettings, configuration );
    }

    @FunctionalInterface
    private interface AccessorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexAccessor<KEY,VALUE> create( PageCache pageCache, FileSystemAbstraction fs, IndexFiles indexFiles, IndexLayout<KEY,VALUE> layout,
                RecoveryCleanupWorkCollector recoveryCleanupWorkCollector, IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor ) throws IOException;
    }
}
