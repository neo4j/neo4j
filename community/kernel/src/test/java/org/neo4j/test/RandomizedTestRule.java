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
package org.neo4j.test;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

public class RandomizedTestRule implements TestRule
{
    private Random random;
    private long seed;

    @Override
    public Statement apply( final Statement base, final Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                String key = description.getClassName() + ".SEED";
                seed = Long.getLong( key, ThreadLocalRandom.current().nextLong() );
                random = new Random( seed );
                try
                {
                    base.evaluate();
                }
                catch ( Throwable t )
                {
                    System.err.println( description.getDisplayName() + " failed with random seed = " + seed +
                                        "\n  To re-run the test with the same seed, set the system property:" +
                                        "\n  -D" + key + "=" + seed );
                    throw t;
                }
                finally
                {
                    random = null;
                }
            }
        };
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[seed=" + seed + "]";
    }

    public void nextBytes( byte[] bytes )
    {
        random.nextBytes( bytes );
    }

    public int nextInt()
    {
        return random.nextInt();
    }

    public int nextInt( int n )
    {
        return random.nextInt( n );
    }

    public long nextLong()
    {
        return random.nextLong();
    }

    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    public float nextFloat()
    {
        return random.nextFloat();
    }

    public double nextDouble()
    {
        return random.nextDouble();
    }

    public double nextGaussian()
    {
        return random.nextGaussian();
    }
}
