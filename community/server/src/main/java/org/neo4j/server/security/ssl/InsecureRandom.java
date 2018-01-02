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
package org.neo4j.server.security.ssl;

import io.netty.util.internal.ThreadLocalRandom;

import java.util.Random;

public class InsecureRandom extends java.security.SecureRandom
{
    @Override
    public String getAlgorithm() {
        return "insecure";
    }

    @Override
    public void setSeed(byte[] seed) { }

    @Override
    public void setSeed(long seed) { }

    @Override
    public void nextBytes(byte[] bytes) {
        random().nextBytes(bytes);
    }

    @Override
    public byte[] generateSeed(int numBytes) {
        byte[] seed = new byte[numBytes];
        random().nextBytes(seed);
        return seed;
    }

    @Override
    public int nextInt() {
        return random().nextInt();
    }

    @Override
    public int nextInt(int n) {
        return random().nextInt(n);
    }

    @Override
    public boolean nextBoolean() {
        return random().nextBoolean();
    }

    @Override
    public long nextLong() {
        return random().nextLong();
    }

    @Override
    public float nextFloat() {
        return random().nextFloat();
    }

    @Override
    public double nextDouble() {
        return random().nextDouble();
    }

    @Override
    public double nextGaussian() {
        return random().nextGaussian();
    }

    private static Random random() {
        return ThreadLocalRandom.current();
    }
}
