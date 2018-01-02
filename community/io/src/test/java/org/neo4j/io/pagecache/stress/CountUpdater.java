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
package org.neo4j.io.pagecache.stress;

import static org.neo4j.io.pagecache.stress.StressTestRecord.SizeOfCounter;

import org.neo4j.io.pagecache.PageCursor;

public class CountUpdater
{
    private final int countersPerRecord;

    public CountUpdater( int countersPerRecord )
    {
        this.countersPerRecord = countersPerRecord;
    }

    public void updateCount( PageCursor cursor, int recordNumber, int counterNumber )
    {
        setOffset( cursor, recordNumber, counterNumber );
        long count = cursor.getLong();
        setOffset( cursor, recordNumber, counterNumber );
        cursor.putLong( count + 1 );
        setOffset( cursor, recordNumber, countersPerRecord );
        long checksum = cursor.getLong();
        setOffset( cursor, recordNumber, countersPerRecord );
        cursor.putLong( checksum + 1 );
    }

    private void setOffset( PageCursor cursor, int recordNumber, int counterNumber )
    {
        cursor.setOffset( (recordNumber * (countersPerRecord + 1) + counterNumber) * SizeOfCounter );
    }
}
