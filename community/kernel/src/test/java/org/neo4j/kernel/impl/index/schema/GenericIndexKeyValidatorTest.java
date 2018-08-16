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

import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.values.storable.DateValue.epochDate;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.stringValue;

public class GenericIndexKeyValidatorTest
{
    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldNotBotherSerializingToRealBytesIfFarFromThreshold()
    {
        // given
        Layout<CompositeGenericKey,NativeIndexValue> layout = mock( Layout.class );
        doThrow( RuntimeException.class ).when( layout ).newKey();
        GenericIndexKeyValidator validator = new GenericIndexKeyValidator( 120, layout );

        // when
        validator.validate( new Value[]{intValue( 10 ), epochDate( 100 ), stringValue( "abc" )} );

        // then no exception should have been thrown
    }

    @Test
    public void shouldInvolveSerializingToRealBytesIfMayCrossThreshold()
    {
        // given
        Layout<CompositeGenericKey,NativeIndexValue> layout = mock( Layout.class );
        when( layout.newKey() ).thenReturn( new CompositeGenericKey( 3 ) );
        GenericIndexKeyValidator validator = new GenericIndexKeyValidator( 48, layout );

        // when
        try
        {
            validator.validate( new Value[]{intValue( 10 ), epochDate( 100 ), stringValue( "abcdefghijklmnopqrstuvw" )} );
            fail( "Should have failed" );
        }
        catch ( IllegalArgumentException e )
        {
            // then good
            assertThat( e.getMessage(), containsString( "abcdefghijklmnopqrstuvw" ) );
            verify( layout, times( 1 ) ).newKey();
        }
    }

    @Test
    public void shouldReportCorrectValidationErrorsOnRandomlyGeneratedValues()
    {
        // given
        int slots = random.nextInt( 1, 6 );
        int maxLength = random.nextInt( 15, 30 ) * slots;
        GenericLayout layout = new GenericLayout( slots );
        GenericIndexKeyValidator validator = new GenericIndexKeyValidator( maxLength, layout );
        CompositeGenericKey key = layout.newKey();

        int countOk = 0;
        int countNotOk = 0;
        for ( int i = 0; i < 100; i++ )
        {
            // when
            Value[] tuple = generateValueTuple( slots );
            boolean isOk;
            try
            {
                validator.validate( tuple );
                isOk = true;
                countOk++;
            }
            catch ( IllegalArgumentException e )
            {
                isOk = false;
                countNotOk++;
            }
            int actualSize = actualSize( tuple, key );
            boolean manualIsOk = actualSize <= maxLength;

            // then
            if ( manualIsOk != isOk )
            {
                fail( format( "Validator not validating %s correctly. Manual validation on actual key resulted in %b whereas validator said %b",
                        Arrays.toString( tuple ), manualIsOk, isOk ) );
            }
        }

        // then a little meta assertion, that the generated parameters in this test are good so that it test at least some on each side of the threshold
        assertThat( countOk, greaterThan( 0 ) );
        assertThat( countNotOk, greaterThan( 0 ) );
    }

    private static int actualSize( Value[] tuple, CompositeGenericKey key )
    {
        key.initialize( 0 );
        for ( int i = 0; i < tuple.length; i++ )
        {
            key.initFromValue( i, tuple[i], NativeIndexKey.Inclusion.NEUTRAL );
        }
        return key.size();
    }

    private Value[] generateValueTuple( int slots )
    {
        Value[] tuple = new Value[slots];
        for ( int j = 0; j < slots; j++ )
        {
            do
            {
                // TODO remember to remove this when generic layout gets support for spatial values
                tuple[j] = random.nextValue();
            }
            while ( Values.isGeometryValue( tuple[j] ) ||
                    (tuple[j].isSequenceValue() && Values.isGeometryValue( (Value) ((SequenceValue) tuple[j]).value( 0 ) )) );
        }
        return tuple;
    }
}
