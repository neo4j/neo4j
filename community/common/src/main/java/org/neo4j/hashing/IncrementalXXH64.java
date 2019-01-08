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
 * @see HashFunction#incrementalXXH64()
 */
class IncrementalXXH64 implements HashFunction
{
    static final HashFunction INSTANCE = new IncrementalXXH64();

    private static final long Prime1 = -7046029288634856825L;
    private static final long Prime2 = -4417276706812531889L;
    private static final long Prime3 = 1609587929392839161L;
    private static final long Prime4 = -8796714831421723037L;
    private static final long Prime5 = 2870177450012600261L;

    private IncrementalXXH64()
    {
    }

    @Override
    public long initialise( long seed )
    {
        return seed + Prime5;
    }

    @Override
    public long update( long hash, long block )
    {
        hash += 8;
        block *= Prime2;
        block = Long.rotateLeft( block, 31 );
        block *= Prime1;
        hash ^= block;
        hash = Long.rotateLeft( hash, 27 ) * Prime1 + Prime4;
        return hash;
    }

    @Override
    public long finalise( long hash )
    {
        hash ^= hash >>> 33;
        hash *= Prime2;
        hash ^= hash >>> 29;
        hash *= Prime3;
        hash ^= hash >>> 32;
        return hash;
    }
}
