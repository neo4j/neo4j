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

import java.time.ZoneId;
import java.time.ZoneOffset;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.StubPageCursor;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.Value;

import static org.junit.Assert.assertEquals;

public class ZonedDateTimeLayoutTest
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

        ZonedDateTimeLayout layout = new ZonedDateTimeLayout();
        PageCursor cursor = new StubPageCursor( 0, 8 * 1024 );
        ZonedDateTimeSchemaKey writeKey = layout.newKey();
        ZonedDateTimeSchemaKey readKey = layout.newKey();

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
            layout.readKey( cursor, readKey, ZonedDateTimeSchemaKey.SIZE );
            assertEquals( value, readKey.asValue() );
        }
    }
}
