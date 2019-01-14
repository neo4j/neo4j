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
package org.neo4j.kernel.impl.store;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.store.format.standard.PropertyRecordFormat;
import org.neo4j.test.Randoms;
import org.neo4j.test.rule.RandomRule;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static org.junit.Assert.assertEquals;

public class PropertyValueRecordSizeCalculatorTest
{
    private static final int PROPERTY_RECORD_SIZE = PropertyRecordFormat.RECORD_SIZE;
    private static final int DYNAMIC_RECORD_SIZE = 120;

    @Rule
    public final RandomRule random = new RandomRule();

    @Test
    public void shouldIncludePropertyRecordSize()
    {
        // given
        PropertyValueRecordSizeCalculator calculator = newCalculator();

        // when
        int size = calculator.applyAsInt( new Value[] {Values.of( 10 )} );

        // then
        assertEquals( PropertyRecordFormat.RECORD_SIZE, size );
    }

    @Test
    public void shouldIncludeDynamicRecordSizes()
    {
        // given
        PropertyValueRecordSizeCalculator calculator = newCalculator();

        // when
        int size = calculator.applyAsInt( new Value[] {Values.of( string( 80 ) ), Values.of( new String[] {string( 150 )} )} );

        // then
        assertEquals( PROPERTY_RECORD_SIZE + DYNAMIC_RECORD_SIZE + DYNAMIC_RECORD_SIZE * 2, size );
    }

    @Test
    public void shouldSpanMultiplePropertyRecords()
    {
        // given
        PropertyValueRecordSizeCalculator calculator = newCalculator();

        // when
        int size = calculator.applyAsInt( new Value[] {
                Values.of( 10 ),                          // 1 block  go to record 1
                Values.of( "test" ),                      // 1 block
                Values.of( (byte) 5 ),                    // 1 block
                Values.of( string( 80 ) ),                // 1 block
                Values.of( "a bit longer short string" ), // 3 blocks go to record 2
                Values.of( 1234567890123456789L ),        // 2 blocks go to record 3
                Values.of( 5 ),                           // 1 block
                Values.of( "value" )                      // 1 block
        } );

        // then
        assertEquals( PROPERTY_RECORD_SIZE * 3 + DYNAMIC_RECORD_SIZE, size );
    }

    private String string( int length )
    {
        return random.string( length, length, Randoms.CSA_LETTERS_AND_DIGITS );
    }

    private PropertyValueRecordSizeCalculator newCalculator()
    {
        return new PropertyValueRecordSizeCalculator( PROPERTY_RECORD_SIZE,
                DYNAMIC_RECORD_SIZE, DYNAMIC_RECORD_SIZE - 10,
                DYNAMIC_RECORD_SIZE, DYNAMIC_RECORD_SIZE - 10 );
    }
}
