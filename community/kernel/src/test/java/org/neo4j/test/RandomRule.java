/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Random;

import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Exceptions.withMessage;

/**
 * Like a {@link Random} but guarantees to include the seed with the test failure, which helps
 * greatly in debugging.
 */
public class RandomRule implements TestRule
{
    private Long specificSeed;
    private long seed;
    private Random random;

    public RandomRule withSeed( long seed )
    {
        this.specificSeed = seed;
        return this;
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                seed = specificSeed == null ? currentTimeMillis() : specificSeed;
                reset();
                try
                {
                    base.evaluate();
                }
                catch ( Throwable t )
                {
                    throw withMessage( t, t.getMessage() + ": random seed used:" + seed );
                }
            }
        };
    }

    public void reset()
    {
        random = new Random( seed );
    }

    public void nextBytes( byte[] bytes )
    {
        random.nextBytes( bytes );
    }

    public boolean nextBoolean()
    {
        return random.nextBoolean();
    }

    public double nextDouble()
    {
        return random.nextDouble();
    }

    public float nextFloat()
    {
        return random.nextFloat();
    }

    public int nextInt()
    {
        return random.nextInt();
    }

    public int nextInt( int n )
    {
        return random.nextInt( n );
    }

    /**
     * For example nextIntBetween( 2, 8 ) can give values between 2-8, all included.
     */
    public int nextIntBetween( int lowIncluded, int highIncluded )
    {
        int diff = highIncluded-lowIncluded;
        assert diff > 0;
        return lowIncluded + random.nextInt( diff+1 );
    }

    public double nextGaussian()
    {
        return random.nextGaussian();
    }

    public long nextLong()
    {
        return random.nextLong();
    }
}
