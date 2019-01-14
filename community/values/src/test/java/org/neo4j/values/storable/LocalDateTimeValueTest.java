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
package org.neo4j.values.storable;

import org.junit.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalDateTimeValue.parse;
import static org.neo4j.values.storable.LocalTimeValue.localTime;
import static org.neo4j.values.utils.AnyValueTestUtil.assertEqual;
import static org.neo4j.values.utils.AnyValueTestUtil.assertNotEqual;

public class LocalDateTimeValueTest
{
    @Test
    public void shouldParseDate()
    {
        assertEquals(
                localDateTime( date( 2017, 12, 17 ), localTime( 17, 14, 35, 123456789 ) ),
                parse( "2017-12-17T17:14:35.123456789" ) );
    }

    @Test
    public void shouldWriteDateTime()
    {
        // given
        for ( LocalDateTimeValue value : new LocalDateTimeValue[] {
                localDateTime( date( 2017, 3, 26 ), localTime( 1, 0, 0, 0 ) ),
                localDateTime( date( 2017, 3, 26 ), localTime( 2, 0, 0, 0 ) ),
                localDateTime( date( 2017, 3, 26 ), localTime( 3, 0, 0, 0 ) ),
                localDateTime( date( 2017, 10, 29 ), localTime( 2, 0, 0, 0 ) ),
                localDateTime( date( 2017, 10, 29 ), localTime( 3, 0, 0, 0 ) ),
                localDateTime( date( 2017, 10, 29 ), localTime( 4, 0, 0, 0 ) ),
        } )
        {
            List<LocalDateTimeValue> values = new ArrayList<>( 1 );
            ValueWriter<RuntimeException> writer = new ThrowingValueWriter.AssertOnly()
            {
                @Override
                public void writeLocalDateTime( LocalDateTime localDateTime )
                {
                    values.add( localDateTime( localDateTime ) );
                }
            };

            // when
            value.writeTo( writer );

            // then
            assertEquals( singletonList( value ), values );
        }
    }

    @Test
    public void shouldEqualItself()
    {
        assertEqual( localDateTime( 2018, 1, 31, 10, 52, 5, 6 ), localDateTime( 2018, 1, 31, 10, 52, 5, 6 ) );
    }

    @Test
    public void shouldNotEqualOther()
    {
        assertNotEqual( localDateTime( 2018, 1, 31, 10, 52, 5, 6 ), localDateTime( 2018, 1, 31, 10, 52, 5, 7 ) );
    }
}
