/*
 * Copyright (c) "Neo4j"
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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static java.util.Collections.singleton;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.memory.ByteBufferFactory.heapBufferFactory;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;
import static org.neo4j.kernel.impl.api.index.PhaseTracker.nullInstance;
import static org.neo4j.values.storable.ValueType.CARTESIAN_POINT;
import static org.neo4j.values.storable.ValueType.CARTESIAN_POINT_3D;
import static org.neo4j.values.storable.ValueType.GEOGRAPHIC_POINT;
import static org.neo4j.values.storable.ValueType.GEOGRAPHIC_POINT_3D;

@ExtendWith( RandomExtension.class )
public class PointBlockBasedIndexPopulatorUpdatesTest extends BlockBasedIndexPopulatorUpdatesTest<PointKey>
{
    private static final ValueType[] supportedTypes = new ValueType[]{CARTESIAN_POINT, CARTESIAN_POINT_3D, GEOGRAPHIC_POINT, GEOGRAPHIC_POINT_3D};
    private final Config config = Config.defaults();
    private final StandardConfiguration configuration = new StandardConfiguration();
    private final IndexSpecificSpaceFillingCurveSettings spatialSettings = IndexSpecificSpaceFillingCurveSettings.fromConfig( config );
    private final PointLayout layout = new PointLayout( spatialSettings );
    @Inject
    private RandomSupport random;

    @Override
    IndexType indexType()
    {
        return IndexType.POINT;
    }

    @Override
    BlockBasedIndexPopulator<PointKey> instantiatePopulator( IndexDescriptor indexDescriptor ) throws IOException
    {
        PointBlockBasedIndexPopulator populator =
                new PointBlockBasedIndexPopulator( databaseIndexContext, indexFiles, layout, indexDescriptor, spatialSettings, configuration, false,
                        heapBufferFactory( (int) kibiBytes( 40 ) ), config, EmptyMemoryTracker.INSTANCE );
        populator.create();
        return populator;
    }

    @Override
    Value supportedValue( int identifier )
    {
        return Values.pointValue( CoordinateReferenceSystem.Cartesian, identifier, identifier );
    }

    @Test
    void shouldIgnoreUnsupportedValueTypesInExternalUpdatesBeforeScanCompleted() throws Exception
    {
        // given
        BlockBasedIndexPopulator<PointKey> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            unsupportedTypes().forEach( unsupportedType -> externalUpdate( populator, random.nextValue( unsupportedType ), 1 ) );
            populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
        }
        finally
        {
            populator.close( true, NULL );
        }

        // then
        assertEmpty();
    }

    @Test
    void shouldIgnoreUnsupportedValueTypesInExternalUpdatesAfterScanCompleted() throws Exception
    {
        // given
        BlockBasedIndexPopulator<PointKey> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
            unsupportedTypes().forEach( unsupportedType -> externalUpdate( populator, random.nextValue( unsupportedType ), 1 ) );
        }
        finally
        {
            populator.close( true, NULL );
        }

        // then
        assertEmpty();
    }

    @Test
    void shouldIgnoreUnsupportedValueTypesInScan() throws Exception
    {
        // given
        BlockBasedIndexPopulator<PointKey> populator = instantiatePopulator( INDEX_DESCRIPTOR );
        try
        {
            // when
            unsupportedTypes().forEach( unsupportedType ->
            {
                IndexEntryUpdate<IndexDescriptor> update = IndexEntryUpdate.add( 1, INDEX_DESCRIPTOR, random.nextValue( unsupportedType ) );
                populator.add( singleton( update ), NULL );
            } );
            populator.scanCompleted( nullInstance, populationWorkScheduler, NULL );
        }
        finally
        {
            populator.close( true, NULL );
        }

        // then
        assertEmpty();
    }

    private void assertEmpty() throws Exception
    {
        try ( var accessor = pointAccessor();
              var allEntriesReader = accessor.newAllEntriesValueReader( NULL ) )
        {
            assertThat( allEntriesReader.iterator().hasNext() ).as( "has values" ).isFalse();
        }
    }

    private PointIndexAccessor pointAccessor()
    {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        return new PointIndexAccessor( databaseIndexContext, indexFiles, layout, cleanup, INDEX_DESCRIPTOR, spatialSettings, configuration );
    }

    private static Stream<ValueType> unsupportedTypes()
    {
        return Arrays.stream( ValueType.values() )
                .filter( type -> !ArrayUtil.contains( supportedTypes, type ) );
    }
}
