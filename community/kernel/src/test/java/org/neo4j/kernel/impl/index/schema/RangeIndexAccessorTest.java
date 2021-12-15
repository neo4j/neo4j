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
import java.util.Iterator;

import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.exceptions.schema.IndexNotApplicableKernelException;
import org.neo4j.internal.kernel.api.security.AccessMode;
import org.neo4j.internal.schema.IndexCapability;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.storageengine.api.schema.SimpleEntityValueClient;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueType;
import org.neo4j.values.storable.Values;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker.writable;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.constrained;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unorderedValues;
import static org.neo4j.internal.kernel.api.QueryContext.NULL_CONTEXT;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.SchemaDescriptors.forLabel;
import static org.neo4j.kernel.impl.index.schema.ValueCreatorUtil.FRACTION_DUPLICATE_NON_UNIQUE;

class RangeIndexAccessorTest extends GenericNativeIndexAccessorTests<RangeKey>
{
    private static final IndexDescriptor INDEX_DESCRIPTOR = forSchema( forLabel( 42, 666 ) ).withIndexType( IndexType.RANGE )
                                                                                            .withIndexProvider( RangeIndexProvider.DESCRIPTOR )
                                                                                            .withName( "index" )
                                                                                            .materialise( 0 );
    private static final ValueType[] SUPPORTED_TYPES = ValueType.values();
    private static final RangeLayout LAYOUT = new RangeLayout( 1 );
    private static final IndexCapability INDEX_CAPABILITY = RangeIndexProvider.CAPABILITY;

    @Override
    NativeIndexAccessor<RangeKey> createAccessor( PageCache pageCache )
    {
        RecoveryCleanupWorkCollector cleanup = RecoveryCleanupWorkCollector.immediate();
        DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs, DEFAULT_DATABASE_NAME ).withReadOnlyChecker( writable() ).build();
        return new RangeIndexAccessor( context, indexFiles, layout, cleanup, INDEX_DESCRIPTOR, tokenNameLookup );
    }

    @Override
    IndexCapability indexCapability()
    {
        return INDEX_CAPABILITY;
    }

    @Override
    boolean supportsGeometryRangeQueries()
    {
        return false;
    }

    @Override
    ValueCreatorUtil<RangeKey> createValueCreatorUtil()
    {
        return new ValueCreatorUtil<>( INDEX_DESCRIPTOR, SUPPORTED_TYPES, FRACTION_DUPLICATE_NON_UNIQUE );
    }

    @Override
    IndexDescriptor indexDescriptor()
    {
        return INDEX_DESCRIPTOR;
    }

    @Override
    RangeLayout layout()
    {
        return LAYOUT;
    }

    @Test
    void readerShouldThrowOnGeometryRangeQueries()
    {
        PropertyIndexQuery.RangePredicate<?> geometryRangePredicate = PropertyIndexQuery.range( 0, CoordinateReferenceSystem.CARTESIAN );

        try ( var reader = accessor.newValueReader() )
        {
            IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> reader.query( new SimpleEntityValueClient(),
                                                                                                           NULL_CONTEXT,
                                                                                                           AccessMode.Static.ACCESS,
                                                                                                           unorderedValues(),
                                                                                                           geometryRangePredicate ) );
            assertThat( e ).hasMessageContaining( "Tried to query index with illegal query. Geometry range predicate is not allowed" );
        }
    }

    @Test
    void readerShouldThrowOnStringSuffixQueries()
    {
        PropertyIndexQuery.StringSuffixPredicate suffixPredicate = PropertyIndexQuery.stringSuffix( 0, Values.stringValue( "myValue" ) );

        try ( var reader = accessor.newValueReader() )
        {
            IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> reader.query( new SimpleEntityValueClient(),
                                                                                                           NULL_CONTEXT,
                                                                                                           AccessMode.Static.ACCESS,
                                                                                                           unorderedValues(),
                                                                                                           suffixPredicate ) );
            assertThat( e ).hasMessageContaining( "Tried to query index with illegal query. STRING_SUFFIX predicate is not allowed" );
        }
    }

    @Test
    void readerShouldThrowOnStringContainsQueries()
    {
        PropertyIndexQuery.StringContainsPredicate containsPredicate = PropertyIndexQuery.stringContains( 0, Values.stringValue( "myValue" ) );

        try ( var reader = accessor.newValueReader() )
        {
            IllegalArgumentException e = assertThrows( IllegalArgumentException.class, () -> reader.query( new SimpleEntityValueClient(),
                                                                                                           NULL_CONTEXT,
                                                                                                           AccessMode.Static.ACCESS,
                                                                                                           unorderedValues(),
                                                                                                           containsPredicate ) );
            assertThat( e ).hasMessageContaining( "Tried to query index with illegal query. STRING_CONTAINS predicate is not allowed" );
        }
    }

    @Test
    void shouldRespectIndexOrderForGeometryTypes() throws Exception
    {
        // given
        int nUpdates = 10000;
        ValueType[] types = supportedTypesForGeometry();
        Iterator<ValueIndexEntryUpdate<IndexDescriptor>> randomUpdateGenerator =
                valueCreatorUtil.randomUpdateGenerator( random, types );
        //noinspection unchecked
        ValueIndexEntryUpdate<IndexDescriptor>[] someUpdates = new ValueIndexEntryUpdate[nUpdates];
        for ( int i = 0; i < nUpdates; i++ )
        {
            someUpdates[i] = randomUpdateGenerator.next();
        }
        processAll( someUpdates );
        Value[] allValues = ValueCreatorUtil.extractValuesFromUpdates( someUpdates );

        // when
        try ( var reader = accessor.newValueReader() )
        {
            final PropertyIndexQuery.ExistsPredicate exists = PropertyIndexQuery.exists( 0 );

            expectIndexOrder( allValues, reader, IndexOrder.ASCENDING, exists );
            expectIndexOrder( allValues, reader, IndexOrder.DESCENDING, exists );
        }
    }

    private static void expectIndexOrder( Value[] allValues, ValueIndexReader reader, IndexOrder supportedOrder,
            PropertyIndexQuery.ExistsPredicate supportedQuery ) throws IndexNotApplicableKernelException
    {
        if ( supportedOrder == IndexOrder.ASCENDING )
        {
            Arrays.sort( allValues, Values.COMPARATOR );
        }
        else if ( supportedOrder == IndexOrder.DESCENDING )
        {
            Arrays.sort( allValues, Values.COMPARATOR.reversed() );
        }
        SimpleEntityValueClient client = new SimpleEntityValueClient();
        reader.query( client, NULL_CONTEXT, AccessMode.Static.READ, constrained( supportedOrder, true ), supportedQuery );
        int i = 0;
        while ( client.next() )
        {
            assertEquals( allValues[i++], client.values[0], "values in order" );
        }
        assertEquals( i, allValues.length, "found all values" );
    }

    private ValueType[] supportedTypesForGeometry()
    {
        return RandomValues.excluding( valueCreatorUtil.supportedTypes(),
                t -> t.valueGroup != ValueGroup.GEOMETRY &&
                     t.valueGroup != ValueGroup.GEOMETRY_ARRAY );
    }
}
