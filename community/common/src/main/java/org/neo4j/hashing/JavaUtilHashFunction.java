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
package org.neo4j.hashing;

/**
 * @see HashFunction#javaUtilHashing()
 */
class JavaUtilHashFunction implements HashFunction
{
    static final HashFunction INSTANCE = new JavaUtilHashFunction();

    private JavaUtilHashFunction()
    {
    }

    @Override
    public long initialise( long seed )
    {
        return seed;
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
        int h = (int) ((value >>> 32) ^ value);
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }
}
