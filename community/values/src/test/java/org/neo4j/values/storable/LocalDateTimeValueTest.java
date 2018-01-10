/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import static org.junit.Assert.assertEquals;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.LocalDateTimeValue.localDateTime;
import static org.neo4j.values.storable.LocalDateTimeValue.parse;
import static org.neo4j.values.storable.LocalTimeValue.localTime;

public class LocalDateTimeValueTest
{
    @Test
    public void shouldParseDate() throws Exception
    {
        assertEquals(
                localDateTime( date( 2017, 12, 17 ), localTime( 17, 14, 35, 123456789 ) ),
                parse( "2017-12-17T17:14:35.123456789" ) );
    }
}
