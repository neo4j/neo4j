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

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueGroup;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;
import static org.neo4j.values.storable.ValueType.CARTESIAN_POINT;
import static org.neo4j.values.storable.ValueType.CARTESIAN_POINT_3D;
import static org.neo4j.values.storable.ValueType.GEOGRAPHIC_POINT;
import static org.neo4j.values.storable.ValueType.GEOGRAPHIC_POINT_3D;

class PointIndexAccessorTest extends NativeIndexAccessorTests<PointKey>
{
    private static final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings =
            IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
    private static final StandardConfiguration configuration = new StandardConfiguration();
    private static final IndexDescriptor indexDescriptor = forSchema( forLabel( 42, 666 ) ).withIndexType( IndexType.POINT )
                                                                                           .withName( "index" ).materialise( 0 );

    private static final ValueType[] supportedTypes = new ValueType[]{CARTESIAN_POINT, CARTESIAN_POINT_3D, GEOGRAPHIC_POINT, GEOGRAPHIC_POINT_3D};
    private final IndexLayoutFactory<PointKey> indexLayoutFactory = () -> new PointLayout( spaceFillingCurveSettings );

    @Override
    ValueCreatorUtil<PointKey> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( indexDescriptor, supportedTypes, FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    boolean supportsGeometryRangeQueries()
    {
        return true;
    }

    @Override
    IndexAccessor createAccessor( PageCache pageCache )
    {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs, DEFAULT_DATABASE_NAME ).withReadOnlyChecker( writable() ).build();
        return new PointIndexAccessor( context, indexFiles, layout, cleanup, indexDescriptor, spaceFillingCurveSettings, configuration );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    @Override
    IndexLayout<PointKey> createLayout()
    {
        return indexLayoutFactory.create();
    }

    @ParameterizedTest
    @MethodSource( "unsupportedPredicates" )
    void readerShouldThrowOnUnsupportedPredicates( PropertyIndexQuery predicate )
    {
        try ( var reader = accessor.newValueReader() )
        {
            IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> reader.query( new SimpleEntityValueClient(),
                    NULL_CONTEXT,
                    AccessMode.Static.ACCESS,
                    unorderedValues(),
                    predicate ) );
            assertThat( e ).hasMessageContaining( "Tried to query index with illegal query. Only geometry range predicate and exact predicate are " +
                    "supported by a point index. Query was:" );
        }
    }

    @ParameterizedTest
    @MethodSource( "unsupportedOrders" )
    void readerShouldThrowOnUnsupportedOrder( IndexOrder indexOrder )
    {
        try ( var reader = accessor.newValueReader() )
        {
            PropertyIndexQuery.ExactPredicate unsupportedQuery = PropertyIndexQuery.exact( 0, PointValue.MAX_VALUE );

            var e = assertThrows( IllegalArgumentException.class, () ->
                    reader.query( new SimpleEntityValueClient(), NULL_CONTEXT, AccessMode.Static.ACCESS, constrained( indexOrder, false ), unsupportedQuery ) );
            assertThat( e.getMessage() ).contains( "Tried to query a point index with order. Order is not supported by a point index" );
        }
    }

    @ParameterizedTest
    @MethodSource( "unsupportedTypes" )
    void updaterShouldIgnoreUnsupportedTypes( ValueType unsupportedType ) throws Exception
    {
        // Given
        // Empty index
        try ( var reader = accessor.newAllEntriesValueReader( CursorContext.NULL ) )
        {
            assertThat( reader.iterator().hasNext() ).as( "has values" ).isFalse();
        }

        // When
        // Processing unsupported values
        try ( var updater = accessor.newUpdater( IndexUpdateMode.ONLINE, CursorContext.NULL ) )
        {
            // Then
            // We should not throw
            Value unsupportedValue = random.nextValue( unsupportedType );
            updater.process( IndexEntryUpdate.add( random.nextInt(), indexDescriptor, unsupportedValue ) );
        }

        // And then
        // Index should still be empty
        try ( var reader = accessor.newAllEntriesValueReader( CursorContext.NULL ) )
        {
            assertThat( reader.iterator().hasNext() ).as( "has values" ).isFalse();
        }
    }

    private static Stream<Arguments> unsupportedPredicates()
    {
        return Stream.of(
                PropertyIndexQuery.exists( 0 ),
                PropertyIndexQuery.range( 0, ValueGroup.UNKNOWN ),
                PropertyIndexQuery.stringPrefix( 0, Values.stringValue( "myValue" ) ),
                PropertyIndexQuery.stringSuffix( 0, Values.stringValue( "myValue" ) ),
                PropertyIndexQuery.stringContains( 0, Values.stringValue( "myValue" ) ),
                PropertyIndexQuery.fulltextSearch( "myValue" )
        ).map( Arguments::of );
    }

    private static Stream<Arguments> unsupportedOrders()
    {
        return Stream.of(
                IndexOrder.DESCENDING,
                IndexOrder.ASCENDING
        ).map( Arguments::of );
    }

    private static Stream<ValueType> unsupportedTypes()
    {
        return Arrays.stream( ValueType.values() )
                .filter( type -> !ArrayUtil.contains( supportedTypes, type ) );
    }
}
