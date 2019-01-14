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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.api.schema.index.TestIndexDescriptorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;

public class NativeIndexPopulatorTest
{
    private static Collection<Object[]> allPopulators()
    {
        return Arrays.asList( new Object[][]{
                {"Number",
                        numberPopulatorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.NUMBER ),
                        (IndexLayoutFactory) NumberLayoutNonUnique::new
                },
                {"String",
                        (PopulatorFactory) StringIndexPopulator::new,
                        RandomValues.typesOfGroup( ValueGroup.TEXT ),
                        (IndexLayoutFactory) StringLayout::new
                },
                {"Date",
                        temporalPopulatorFactory( ValueGroup.DATE ),
                        RandomValues.typesOfGroup( ValueGroup.DATE ),
                        (IndexLayoutFactory) DateLayout::new
                },
                {"DateTime",
                        temporalPopulatorFactory( ValueGroup.ZONED_DATE_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.ZONED_DATE_TIME ),
                        (IndexLayoutFactory) ZonedDateTimeLayout::new
                },
                {"Duration",
                        temporalPopulatorFactory( ValueGroup.DURATION ),
                        RandomValues.typesOfGroup( ValueGroup.DURATION ),
                        (IndexLayoutFactory) DurationLayout::new
                },
                {"LocalDateTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_DATE_TIME ),
                        (IndexLayoutFactory) LocalDateTimeLayout::new
                },
                {"LocalTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_TIME ),
                        (IndexLayoutFactory) LocalTimeLayout::new
                },
                {"LocalDateTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_DATE_TIME ),
                        (IndexLayoutFactory) LocalDateTimeLayout::new
                },
                {"Time",
                        temporalPopulatorFactory( ValueGroup.ZONED_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.ZONED_TIME ),
                        (IndexLayoutFactory) ZonedTimeLayout::new
                },
                {"Generic",
                        genericPopulatorFactory(),
                        ValueType.values(),
                        (IndexLayoutFactory) () -> new GenericLayout( 1, spaceFillingCurveSettings )
                },
                // todo { Spatial has it's own subclass because it need to override some of the test methods }
        } );
    }

    private static final IndexSpecificSpaceFillingCurveSettingsCache spaceFillingCurveSettings =
            new IndexSpecificSpaceFillingCurveSettingsCache( new ConfiguredSpaceFillingCurveSettingsCache( Config.defaults() ), new HashMap<>() );
    private static final StandardConfiguration configuration = new StandardConfiguration();

    private static PopulatorFactory<NumberIndexKey,NativeIndexValue> numberPopulatorFactory()
    {
        return NumberIndexPopulator::new;
    }

    private static <TK extends NativeIndexSingleValueKey<TK>> PopulatorFactory<TK,NativeIndexValue> temporalPopulatorFactory( ValueGroup temporalValueGroup )
    {
        return ( pageCache, fs, storeFile, layout, monitor, descriptor ) ->
        {
            TemporalIndexFiles.FileLayout<TK> fileLayout = new TemporalIndexFiles.FileLayout<>( storeFile, layout, temporalValueGroup );
            return new TemporalIndexPopulator.PartPopulator<>( pageCache, fs, fileLayout, monitor, descriptor );
        };
    }

    private static PopulatorFactory<GenericKey,NativeIndexValue> genericPopulatorFactory()
    {
        return ( pageCache, fs, storeFile, layout, monitor, descriptor ) ->
                new GenericNativeIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, spaceFillingCurveSettings,
                        SimpleIndexDirectoryStructures.onIndexFile( storeFile ), configuration, false, false );
    }

    @FunctionalInterface
    private interface PopulatorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexPopulator<KEY,VALUE> create( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout,
                IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor ) throws IOException;
    }

    @RunWith( Parameterized.class )
    public static class UniqueTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexPopulatorTests.Unique<KEY,VALUE>
    {
        @Parameterized.Parameters( name = "{index} {0}" )
        public static Collection<Object[]> data()
        {
            return allPopulators();
        }

        @Parameterized.Parameter()
        public String name;

        @Parameterized.Parameter( 1 )
        public PopulatorFactory<KEY,VALUE> populatorFactory;

        @Parameterized.Parameter( 2 )
        public ValueType[] supportedTypes;

        @Parameterized.Parameter( 3 )
        public IndexLayoutFactory<KEY,VALUE> indexLayoutFactory;

        private static final StoreIndexDescriptor uniqueDescriptor = TestIndexDescriptorFactory.uniqueForLabel( 42, 666 ).withId( 0 );

        @Override
        NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException
        {
            return populatorFactory.create( pageCache, fs, getIndexFile(), layout, monitor, indexDescriptor );
        }

        @Override
        ValueCreatorUtil<KEY,VALUE> createValueCreatorUtil()
        {
            return new ValueCreatorUtil<>( uniqueDescriptor, supportedTypes, ValueCreatorUtil.FRACTION_DUPLICATE_UNIQUE );
        }

        @Override
        IndexLayout<KEY,VALUE> createLayout()
        {
            return indexLayoutFactory.create();
        }
    }

    @RunWith( Parameterized.class )
    public static class NonUniqueTest<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue> extends NativeIndexPopulatorTests.NonUnique<KEY,VALUE>
    {
        @Parameterized.Parameters( name = "{index} {0}" )
        public static Collection<Object[]> data()
        {
            return allPopulators();
        }

        @Parameterized.Parameter()
        public String name;

        @Parameterized.Parameter( 1 )
        public PopulatorFactory<KEY,VALUE> populatorFactory;

        @Parameterized.Parameter( 2 )
        public ValueType[] supportedTypes;

        @Parameterized.Parameter( 3 )
        public IndexLayoutFactory<KEY,VALUE> indexLayoutFactory;

        private static final StoreIndexDescriptor nonUniqueDescriptor = TestIndexDescriptorFactory.forLabel( 42, 666 ).withId( 0 );

        @Override
        NativeIndexPopulator<KEY,VALUE> createPopulator() throws IOException
        {
            return populatorFactory.create( pageCache, fs, getIndexFile(), layout, monitor, indexDescriptor );
        }

        @Override
        ValueCreatorUtil<KEY,VALUE> createValueCreatorUtil()
        {
            return new ValueCreatorUtil<>( nonUniqueDescriptor, supportedTypes, ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE );
        }

        @Override
        IndexLayout<KEY,VALUE> createLayout()
        {
            return indexLayoutFactory.create();
        }
    }
}
