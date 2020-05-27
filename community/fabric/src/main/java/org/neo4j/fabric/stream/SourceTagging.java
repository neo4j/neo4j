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
package org.neo4j.fabric.stream;

public class SourceTagging
{
    private static final int ID_MAX_BITS = 50;
    private static final long TAG_MAX_VALUE = 0x3FFF;

    public static long makeSourceTag( long sourceId )
    {
        if ( sourceId < 0 || TAG_MAX_VALUE < sourceId )
        {
            throw new IllegalArgumentException( "Source ids must be in range 0-16383. Got: " + sourceId );
        }
        else
        {
            return shiftToMsb( sourceId );
        }
    }

    private static long shiftToMsb( long value )
    {
        return value << ID_MAX_BITS;
    }

    public static long tagId( long id, long sourceTag )
    {
        return id | sourceTag;
    }

    public static long extractSourceId( long id )
    {
        return id >> ID_MAX_BITS;
    }
}
