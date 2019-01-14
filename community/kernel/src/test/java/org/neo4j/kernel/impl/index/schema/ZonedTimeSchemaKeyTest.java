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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import java.time.ZoneOffset;

import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class ZonedTimeSchemaKeyTest
{
    @Test
    public void compareToSameAsValue()
    {
        Value[] values = {TimeValue.time( 9999, ZoneOffset.of( "+18:00" ) ),
                          TimeValue.time( 10000, ZoneOffset.of( "-18:00" ) ),
                          TimeValue.time( 10000, ZoneOffset.of( "-00:00" ) ),
                          TimeValue.time( 10000, ZoneOffset.of( "+01:00" ) ),
                          TimeValue.time( 10000, ZoneOffset.of( "+03:00" ) ),
                          TimeValue.time( 10000, ZoneOffset.of( "-18:00" ) )};

        ZonedTimeSchemaKey keyI = new ZonedTimeSchemaKey();
        ZonedTimeSchemaKey keyJ = new ZonedTimeSchemaKey();

        for ( Value vi : values )
        {
            for ( Value vj : values )
            {
                vi.writeTo( keyI );
                vj.writeTo( keyJ );

                int expected = Values.COMPARATOR.compare( vi, vj );
                assertEquals( format( "comparing %s and %s", vi, vj ), expected, keyI.compareValueTo( keyJ ) );
            }
        }
    }
}
