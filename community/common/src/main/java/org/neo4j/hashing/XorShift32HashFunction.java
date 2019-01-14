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
package org.neo4j.hashing;

/**
 * @see HashFunction#xorShift32()
 */
class XorShift32HashFunction implements HashFunction
{
    static final XorShift32HashFunction INSTANCE = new XorShift32HashFunction();

    private XorShift32HashFunction()
    {
    }

    @Override
    public long initialise( long seed )
    {
        return 0;
    }

    @Override
    public long update( long intermediateHash, long value )
    {
        return hashSingleValueToInt( intermediateHash + value );
    }

    @Override
    public long finalise( long intermediateHash )
    {
        return intermediateHash;
    }

    @Override
    public int hashSingleValueToInt( long value )
    {
        value ^= value << 21;
        value ^= value >>> 35;
        value ^= value << 4;
        return (int) ((value >>> 32) ^ value);
    }
}
