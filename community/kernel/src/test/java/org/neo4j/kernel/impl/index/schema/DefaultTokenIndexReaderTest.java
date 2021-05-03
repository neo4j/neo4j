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

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.kernel.api.IndexQueryConstraints;
import org.neo4j.internal.kernel.api.TokenPredicate;
import org.neo4j.internal.schema.IndexOrder;
import org.neo4j.storageengine.api.schema.SimpleEntityTokenClient;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.io.pagecache.context.CursorContext.NULL;

@Execution( CONCURRENT )
class DefaultTokenIndexReaderTest
{
    private static final int LABEL_ID = 1;
    private final long[] keys = {0, 1, 3};
    private final long[] bitsets = {0b1000_1000__1100_0010L,
                                    0b0000_0010__0000_1000L,
                                    0b0010_0000__1010_0001L};
    private final long[] expected = {
            // base 0*64 = 0
            1, 6, 7, 11, 15,
            // base 1*64 = 64
            64 + 3, 64 + 9,
            // base 3*64 = 192
            192, 192 + 5, 192 + 7, 192 + 13};
    private GBPTree<TokenScanKey,TokenScanValue> index;

    @BeforeEach
    void setUp() throws IOException
    {
        index = mock( GBPTree.class );

        when( index.seek( any( TokenScanKey.class ), any( TokenScanKey.class ), eq( NULL ) ) )
                .thenAnswer( innvocation ->
                        cursor( innvocation.getArgument( 0, TokenScanKey.class ).idRange <= innvocation.getArgument( 1, TokenScanKey.class ).idRange ) );
    }

    private Seeker<TokenScanKey,TokenScanValue> cursor( boolean ascending ) throws Exception
    {
        Seeker<TokenScanKey,TokenScanValue> cursor = mock( Seeker.class );
        when( cursor.next() ).thenReturn( true, true, true, false );
        when( cursor.key() ).thenReturn(
                key( ascending ? keys[0] : keys[2] ),
                key( keys[1] ),
                key( ascending ? keys[2] : keys[0] ) );
        when( cursor.value() ).thenReturn(
                value( ascending ? bitsets[0] : bitsets[2] ),
                value( bitsets[1] ),
                value( ascending ? bitsets[2] : bitsets[0] ),
                null );
        return cursor;
    }

    @Test
    void shouldFindMultipleEntitiesInEachRange()
    {
        // WHEN
        var reader = new DefaultTokenIndexReader( index );
        SimpleEntityTokenClient tokenClient = new SimpleEntityTokenClient();
        reader.query( tokenClient, unconstrained(), new TokenPredicate( LABEL_ID ), NULL );

        // THEN
        assertArrayEquals( expected, asArray( tokenClient ) );
    }

    @Test
    void shouldFindMultipleWithProgressorAscending()
    {
        // WHEN
        var reader = new DefaultTokenIndexReader( index );
        SimpleEntityTokenClient tokenClient = new SimpleEntityTokenClient();
        reader.query( tokenClient, IndexQueryConstraints.constrained( IndexOrder.ASCENDING, false ), new TokenPredicate( LABEL_ID ), NULL );

        // THEN
        assertArrayEquals( expected, asArray( tokenClient ) );
    }

    @Test
    void shouldFindMultipleWithProgressorDescending()
    {
        // WHEN
        var reader = new DefaultTokenIndexReader( index );
        SimpleEntityTokenClient tokenClient = new SimpleEntityTokenClient();
        reader.query( tokenClient, IndexQueryConstraints.constrained( IndexOrder.DESCENDING, false ), new TokenPredicate( LABEL_ID ), NULL );

        // THEN
        ArrayUtils.reverse( expected );
        assertArrayEquals( expected, asArray( tokenClient ) );
    }

    private static long[] asArray( SimpleEntityTokenClient valueClient )
    {
        MutableLongList result = LongLists.mutable.empty();
        while ( valueClient.next() )
        {
            result.add( valueClient.reference );
        }
        return result.toArray();
    }

    private static TokenScanValue value( long bits )
    {
        TokenScanValue value = new TokenScanValue();
        value.bits = bits;
        return value;
    }

    private static TokenScanKey key( long idRange )
    {
        return new TokenScanKey( LABEL_ID, idRange );
    }

}
