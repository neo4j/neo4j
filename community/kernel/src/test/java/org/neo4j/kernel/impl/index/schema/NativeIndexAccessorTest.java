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

import org.neo4j.configuration.Config;
import org.neo4j.gis.spatial.index.curves.StandardConfiguration;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.index.schema.config.IndexSpecificSpaceFillingCurveSettings;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.ValueType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.helpers.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;

class NativeIndexAccessorTest extends GenericNativeIndexAccessorTests<BtreeKey>
{
    private static final IndexSpecificSpaceFillingCurveSettings spaceFillingCurveSettings =
            IndexSpecificSpaceFillingCurveSettings.fromConfig( Config.defaults() );
    private static final StandardConfiguration configuration = new StandardConfiguration();
    private static final IndexDescriptor indexDescriptor = forSchema( forLabel( 42, 666 ) ).withName( "index" ).materialise( 0 );

    private final ValueType[] supportedTypes = ValueType.values();
    private final IndexLayoutFactory<BtreeKey> indexLayoutFactory = () -> new GenericLayout( 1, spaceFillingCurveSettings );
    private final IndexCapability indexCapability = GenericNativeIndexProvider.CAPABILITY;

    @Override
    NativeIndexAccessor<BtreeKey> createAccessor( PageCache pageCache )
    {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs, DEFAULT_DATABASE_NAME ).withReadOnlyChecker( writable() ).build();
        return new GenericNativeIndexAccessor( context, indexFiles, layout, cleanup, indexDescriptor, spaceFillingCurveSettings, configuration,
                tokenNameLookup );
    }

    @Override
    IndexCapability indexCapability()
    {
        return indexCapability;
    }

    @Override
    boolean supportsGeometryRangeQueries()
    {
        return true;
    }

    @Override
    ValueCreatorUtil<BtreeKey> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( indexDescriptor, supportedTypes, FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        return indexDescriptor;
    }

    @Override
    IndexLayout<BtreeKey> createLayout()
    {
        return indexLayoutFactory.create();
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
}
