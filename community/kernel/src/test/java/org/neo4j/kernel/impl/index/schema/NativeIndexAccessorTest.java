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

import java.util.Arrays;

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ValueType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.function.Predicates.alwaysTrue;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;

class NativeIndexAccessorTest extends GenericNativeIndexAccessorTests<BtreeKey>
{
    private static final IndexSpecificSpaceFillingCurveSettings SPACE_FILLING_CURVE_SETTINGS =
            IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
    private static final StandardConfiguration CONFIGURATION = new StandardConfiguration();
    private static final IndexDescriptor INDEX_DESCRIPTOR = forSchema( forLabel( 42, 666 ) ).withIndexType( IndexType.BTREE )
                                                                                            .withIndexProvider( GenericNativeIndexProvider.DESCRIPTOR )
                                                                                            .withName( "index" )
                                                                                            .materialise( 0 );

    private static final ValueType[] SUPPORTED_TYPES = ValueType.values();
    private static final GenericLayout LAYOUT = new GenericLayout( 1, SPACE_FILLING_CURVE_SETTINGS );
    private static final IndexCapability INDEX_CAPABILITY = GenericNativeIndexProvider.CAPABILITY;

    @Override
    NativeIndexAccessor<BtreeKey> createAccessor( PageCache pageCache )
    {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs, DEFAULT_DATABASE_NAME ).withReadOnlyChecker( writable() ).build();
        return new GenericNativeIndexAccessor( context, indexFiles, layout, cleanup, INDEX_DESCRIPTOR,
                                               SPACE_FILLING_CURVE_SETTINGS, CONFIGURATION, tokenNameLookup );
    }

    @Override
    IndexCapability indexCapability()
    {
        return INDEX_CAPABILITY;
    }

    @Override
    ValueCreatorUtil<BtreeKey> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( INDEX_DESCRIPTOR, SUPPORTED_TYPES, FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        return INDEX_DESCRIPTOR;
    }

    @Override
    GenericLayout layout()
    {
        return LAYOUT;
    }

    @Test
    void throwForUnsupportedIndexOrder()
    {
        // given
        // Unsupported index order for query
        try ( var reader = accessor.newValueReader() )
        {
            IndexOrder unsupportedOrder = IndexOrder.DESCENDING;
            PropertyIndexQuery.ExactPredicate unsupportedQuery = PropertyIndexQuery.exact( 0, PointValue.MAX_VALUE ); // <- Any spatial value would do

            var e = assertThrows( UnsupportedOperationException.class, () ->
                    reader.query( new SimpleEntityValueClient(), NULL_CONTEXT, AccessMode.Static.ACCESS,
                                  constrained( unsupportedOrder, false ), unsupportedQuery ) );
            assertThat( e.getMessage() ).contains( "unsupported order" ).contains( unsupportedOrder.toString() ).contains( unsupportedQuery.toString() );
        }
    }

    @Test
    void shouldReturnMatchingEntriesForPointArrayRangePredicate() throws Exception
    {
        // given
        final var updates = someUpdatesSingleTypeNoDuplicates( ValueType.CARTESIAN_POINT_ARRAY );
        processAll( updates );
        ValueCreatorUtil.sort( updates );

        final int fromInclusive = 2;
        final int toExclusive = updates.length - 1;

        // when
        try ( var reader = accessor.newValueReader();
              var result = query( reader, ValueCreatorUtil.rangeQuery( valueOf( updates[fromInclusive] ), true,
                                                                       valueOf( updates[toExclusive] ), false ) ) )
        {
            // then
            assertEntityIdHits( new long[0], result );
        }
    }
}
