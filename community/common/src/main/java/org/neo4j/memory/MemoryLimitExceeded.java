/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.memory;

import java.text.CharacterIterator;
import java.text.StringCharacterIterator;

import org.neo4j.kernel.api.exceptions.Status;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.General.TransactionMemoryLimit;

public class MemoryLimitExceeded extends RuntimeException implements Status.HasStatus
{

    public MemoryLimitExceeded( long allocation, long limit, long current )
    {
        super( format( "The allocation of %s would use more than the limit %s. Currently using %s", humanReadableByteCountBin( allocation ),
                       humanReadableByteCountBin( limit ), humanReadableByteCountBin( current ) ) );
    }

    @Override
    public Status status()
    {
        return TransactionMemoryLimit;
    }

    private static String humanReadableByteCountBin( long bytes )
    {
        long absB = bytes == Long.MIN_VALUE ? Long.MAX_VALUE : Math.abs( bytes );
        if ( absB < 1024 )
        {
            return bytes + " B";
        }
        long value = absB;
        CharacterIterator ci = new StringCharacterIterator( "KMGTPE" );
        for ( int i = 40; i >= 0 && absB > 0xfffccccccccccccL >> i; i -= 10 )
        {
            value >>= 10;
            ci.next();
        }
        value *= Long.signum( bytes );
        return String.format( "%.1f %ciB", value / 1024.0, ci.current() );
    }
}
