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
package org.neo4j.kernel.impl.index.schema;

import org.junit.Test;

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

public class TemporalLayoutTest
{
    @Test
    public void shouldReadAndWriteConsistentValues()
    {
        Value[] values = {
                DateTimeValue.datetime( 9999, 100,  ZoneId.of( "+18:00" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "-18:00" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneOffset.of( "-17:59:59" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "UTC" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "+01:00" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "Europe/Stockholm" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "+03:00" ) ),
                DateTimeValue.datetime( 10000, 101, ZoneId.of( "-18:00" ) )
        };

        TemporalLayout layout = new TemporalLayout();
        PageCursor cursor = new StubPageCursor( 0, 8 * 1024 );
        TemporalSchemaKey writeKey = layout.newKey();
        TemporalSchemaKey readKey = layout.newKey();

        // Write all
        for ( Value value : values )
        {
            value.writeTo( writeKey );
            layout.writeKey( cursor, writeKey );
        }

        // Read all
        cursor.setOffset( 0 );
        for ( Value value : values )
        {
            layout.readKey( cursor, readKey, 50 );
            assertEquals( value, readKey.asValue() );
        }
    }

    @Test
    public void compareZonedDateTimeToSameAsValue()
    {
        Value[] values = {DateTimeValue.datetime( 9999, 100,  ZoneId.of( "+18:00" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "-18:00" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneOffset.of( "-17:59:59" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "UTC" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "+01:00" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "Europe/Stockholm" ) ),
                DateTimeValue.datetime( 10000, 100, ZoneId.of( "+03:00" ) ),
                DateTimeValue.datetime( 10000, 101, ZoneId.of( "-18:00" ) )};

        TemporalSchemaKey keyI = new TemporalSchemaKey();
        TemporalSchemaKey keyJ = new TemporalSchemaKey();

        int len = values.length;

        for ( int i = 0; i < len; i++ )
        {
            for ( int j = 0; j < len; j++ )
            {
                Value vi = values[i];
                Value vj = values[j];
                vi.writeTo( keyI );
                vj.writeTo( keyJ );

                int expected = Integer.signum( Values.COMPARATOR.compare( vi, vj ) );
                assertEquals( format( "comparing %s and %s", vi, vj ), expected, Integer.signum( i - j ) );
                assertEquals( format( "comparing %s and %s", vi, vj ), expected, Integer.signum( keyI.compareValueTo( keyJ ) ) );
            }
        }
    }

    @Test
    public void compareZonedTimeToSameAsValue()
    {
        Value[] values = {TimeValue.time( 9999, ZoneOffset.of( "+18:00" ) ),
                TimeValue.time( 10000, ZoneOffset.of( "-18:00" ) ),
                TimeValue.time( 10000, ZoneOffset.of( "-00:00" ) ),
                TimeValue.time( 10000, ZoneOffset.of( "+01:00" ) ),
                TimeValue.time( 10000, ZoneOffset.of( "+03:00" ) ),
                TimeValue.time( 10000, ZoneOffset.of( "-18:00" ) )};

        TemporalSchemaKey keyI = new TemporalSchemaKey();
        TemporalSchemaKey keyJ = new TemporalSchemaKey();

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
