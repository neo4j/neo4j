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
package org.neo4j.values.storable;

import java.util.SplittableRandom;

public class SplittableRandomGenerator implements Generator
{
    private final SplittableRandom random;

    SplittableRandomGenerator( SplittableRandom random )
    {
        this.random = random;
    }

    @Override
    public long nextLong()
    {
        return random.nextLong();
    }

    @Override
    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    @Override
    public int nextInt()
    {
        return random.nextInt();
    }

    @Override
    public int nextInt( int bound )
    {
        return random.nextInt( bound );
    }

    @Override
    public float nextFloat()
    {
        //this is a safe cast since nextDouble returns values in [0,1.0)
        return (float) random.nextDouble();
    }

    @Override
    public double nextDouble()
    {
        return random.nextDouble();
    }
}
