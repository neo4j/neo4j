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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;

import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.IndexDirectoryStructure;
import org.neo4j.kernel.api.index.IndexProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.schema.config.ConfiguredSpaceFillingCurveSettingsCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettingsCache;
import org.neo4j.storageengine.api.schema.StoreIndexDescriptor;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;

import static org.neo4j.kernel.impl.index.schema.ByteBufferFactory.heapBufferFactory;

class NativeIndexPopulatorTestCases
{
    static class TestCase<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        final String name;
        final PopulatorFactory<KEY,VALUE> populatorFactory;
        final ValueType[] typesOfGroup;
        final IndexLayoutFactory<KEY,VALUE> indexLayoutFactory;

        TestCase( String name, PopulatorFactory<KEY,VALUE> populatorFactory, ValueType[] typesOfGroup, IndexLayoutFactory<KEY,VALUE> indexLayoutFactory )
        {
            this.name = name;
            this.populatorFactory = populatorFactory;
            this.typesOfGroup = typesOfGroup;
            this.indexLayoutFactory = indexLayoutFactory;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    static Collection<Object[]> allCases()
    {
        return Arrays.asList( new Object[][]{
                {new TestCase<>( "Number",
                        numberPopulatorFactory(),
                        RandomValues.typesOfGroup( ValueGroup.NUMBER ),
                        NumberLayoutNonUnique::new )},
                {new TestCase<>( "String",
                        StringIndexPopulator::new,
                        RandomValues.typesOfGroup( ValueGroup.TEXT ),
                        StringLayout::new )},
                {new TestCase<>( "Date",
                        temporalPopulatorFactory( ValueGroup.DATE ),
                        RandomValues.typesOfGroup( ValueGroup.DATE ),
                        DateLayout::new )},
                {new TestCase<>( "DateTime",
                        temporalPopulatorFactory( ValueGroup.ZONED_DATE_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.ZONED_DATE_TIME ),
                        ZonedDateTimeLayout::new )},
                {new TestCase<>( "Duration",
                        temporalPopulatorFactory( ValueGroup.DURATION ),
                        RandomValues.typesOfGroup( ValueGroup.DURATION ),
                        DurationLayout::new )},
                {new TestCase<>( "LocalDateTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_DATE_TIME ),
                        LocalDateTimeLayout::new )},
                {new TestCase<>( "LocalTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_TIME ),
                        LocalTimeLayout::new )},
                {new TestCase<>( "LocalDateTime",
                        temporalPopulatorFactory( ValueGroup.LOCAL_DATE_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.LOCAL_DATE_TIME ),
                        LocalDateTimeLayout::new )},
                {new TestCase<>( "Time",
                        temporalPopulatorFactory( ValueGroup.ZONED_TIME ),
                        RandomValues.typesOfGroup( ValueGroup.ZONED_TIME ),
                        ZonedTimeLayout::new )},
                {new TestCase<>( "Generic",
                        genericPopulatorFactory(),
                        ValueType.values(),
                        () -> new GenericLayout( 1, spaceFillingCurveSettings ) )},
                {new TestCase<>( "Generic-BlockBased",
                        genericBlockBasedPopulatorFactory(),
                        ValueType.values(),
                        () -> new GenericLayout( 1, spaceFillingCurveSettings ) )}
        } );
        // { Spatial has it's own subclass because it need to override some of the test methods }
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
        {
            IndexDirectoryStructure directoryStructure = SimpleIndexDirectoryStructures.onIndexFile( storeFile );
            IndexDropAction dropAction = new FileSystemIndexDropAction( fs, directoryStructure );
            return new GenericNativeIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, spaceFillingCurveSettings,
                    directoryStructure, configuration, dropAction, false );
        };
    }

    private static PopulatorFactory<GenericKey,NativeIndexValue> genericBlockBasedPopulatorFactory()
    {
        return ( pageCache, fs, storeFile, layout, monitor, descriptor ) ->
        {
            IndexDirectoryStructure directoryStructure = SimpleIndexDirectoryStructures.onIndexFile( storeFile );
            IndexDropAction dropAction = new FileSystemIndexDropAction( fs, directoryStructure );
            return new GenericBlockBasedIndexPopulator( pageCache, fs, storeFile, layout, monitor, descriptor, spaceFillingCurveSettings,
                    directoryStructure, configuration, dropAction, false, heapBufferFactory( 10 * 1024 ) );
        };
    }

    @FunctionalInterface
    public interface PopulatorFactory<KEY extends NativeIndexKey<KEY>, VALUE extends NativeIndexValue>
    {
        NativeIndexPopulator<KEY,VALUE> create( PageCache pageCache, FileSystemAbstraction fs, File storeFile, IndexLayout<KEY,VALUE> layout,
                IndexProvider.Monitor monitor, StoreIndexDescriptor descriptor ) throws IOException;
    }
}
