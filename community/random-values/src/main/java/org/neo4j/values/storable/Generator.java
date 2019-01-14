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
package org.neo4j.values.storable;

/**
 * This class is meant as a gap so that we don't need a direct dependency on a random number generator.
 * <p>
 * For example by wrapping in a generator we can support both {@code java.util.Random} and {@code java.util
 * .SplittableRandom}
 */
public interface Generator
{
    /**
     * Return a pseudorandom normally distributed long
     * @return a pseudorandom normally distributed long
     */
    long nextLong();

    /**
     * Return a pseudorandom normally distributed boolean
     * @return a pseudorandom normally distributed boolean
     */
    boolean nextBoolean();

    /**
     * Return a pseudorandom normally distributed int
     * @return a pseudorandom normally distributed int
     */
    int nextInt();

    /**
     * Return a pseudorandom normally distributed long between 0 (inclusive) and the given bound(exlusive)
     * @param bound the exclusive upper bound for the number generation
     * @return a pseudorandom normally distributed int
     */
    int nextInt( int bound );

    /**
     * Return a pseudorandom normally distributed float from {@code 0.0f} (inclusive) to {@code 1.0f} (exclusive)
     * @return a pseudorandom normally distributed from {@code 0.0f} (inclusive) to {@code 1.0f} (exclusive)
     */
    float nextFloat();

    /**
     * Return a pseudorandom normally distributed double from {@code 0.0} (inclusive) to {@code 1.0} (exclusive)
     * @return a pseudorandom normally distributed double from {@code 0.0} (inclusive) to {@code 1.0} (exclusive)
     */
    double nextDouble();
}
