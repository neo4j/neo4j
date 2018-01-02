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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import static java.lang.Math.log10;
import static java.lang.Math.max;

/**
 * {@link Encoder} that assumes that the entered strings can be parsed to {@link Long} directly.
 */
public class LongEncoder implements Encoder
{
    @Override
    public long encode( Object value )
    {
        long longValue = ((Number)value).longValue();
        long length = numberOfDigits( longValue );
        length = length << 57;
        long returnVal = length | longValue;
        return returnVal;
    }

    private static int numberOfDigits( long value )
    {
        return max( 1, (int)(log10( value ) + 1) );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }
}
