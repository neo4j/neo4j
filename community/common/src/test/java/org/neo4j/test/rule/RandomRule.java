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
package org.neo4j.test.rule;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;

import org.neo4j.helpers.Exceptions;
import org.neo4j.test.Randoms;
import org.neo4j.test.Randoms.Configuration;

import static java.lang.System.currentTimeMillis;

/**
 * Like a {@link Random} but guarantees to include the seed with the test failure, which helps
 * greatly in debugging.
 *
 * Available methods directly on this class include those found in {@link Randoms} and the basic ones in {@link Random}.
 */
public class RandomRule implements TestRule
{
    private long seed;
    private boolean hasGlobalSeed;
    private Random random;
    private Randoms randoms;
    private Configuration config = Randoms.DEFAULT;

    public RandomRule withConfiguration( Randoms.Configuration config )
    {
        this.config = config;
        return this;
    }

    public RandomRule withSeedForAllTests( long seed )
    {
        hasGlobalSeed = true;
        this.seed = seed;
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
                if ( !hasGlobalSeed )
                {
                    Seed methodSeed = description.getAnnotation( Seed.class );
                    if ( methodSeed != null )
                    {
                        seed = methodSeed.value();
                    }
                    else
                    {
                        seed = currentTimeMillis();
                    }
                }
                reset();
                try
                {
                    base.evaluate();
                }
                catch ( Throwable t )
                {
                    if ( t instanceof MultipleFailureException )
                    {
                        MultipleFailureException multipleFailures = (MultipleFailureException) t;
                        for ( Throwable failure : multipleFailures.getFailures() )
                        {
                            enhanceFailureWithSeed( failure );
                        }
                    }
                    else
                    {
                        enhanceFailureWithSeed( t );
                    }
                    throw t;
                }
            }

            private void enhanceFailureWithSeed( Throwable t )
            {
                Exceptions.withMessage( t, t.getMessage() + ": random seed used:" + seed + "L" );
            }
        };
    }

    // ============================
    // Methods from Random
    // ============================

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

    public DoubleStream doubles( int dimension, double minValue, double maxValue )
    {
        return random.doubles( dimension, minValue, maxValue );
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

    public int nextInt( int origin, int bound )
    {
        return random.nextInt( (bound - origin) + 1 ) + origin;
    }

    public IntStream ints( long streamSize, int randomNumberOrigin, int randomNumberBound )
    {
        return random.ints( streamSize, randomNumberOrigin, randomNumberBound );
    }

    public double nextGaussian()
    {
        return random.nextGaussian();
    }

    public long nextLong()
    {
        return random.nextLong();
    }

    public long nextLong( long n )
    {
        return Math.abs( nextLong() ) % n;
    }

    public long nextLong( long origin, long bound )
    {
        return nextLong( (bound - origin) + 1L ) + origin;
    }

    // ============================
    // Methods from Randoms
    // ============================

    public int intBetween( int min, int max )
    {
        return randoms.intBetween( min, max );
    }

    public String string()
    {
        return randoms.string();
    }

    public String string( int minLength, int maxLength, int characterSets )
    {
        return randoms.string( minLength, maxLength, characterSets );
    }

    public char character( int characterSets )
    {
        return randoms.character( characterSets );
    }

    public <T> T[] selection( T[] among, int min, int max, boolean allowDuplicates )
    {
        return randoms.selection( among, min, max, allowDuplicates );
    }

    public <T> T among( T[] among )
    {
        return randoms.among( among );
    }

    public <T> T among( List<T> among )
    {
        return randoms.among( among );
    }

    public <T> void among( List<T> among, Consumer<T> action )
    {
        randoms.among( among, action );
    }

    public Number numberPropertyValue()
    {
        return randoms.numberPropertyValue();
    }

    public Object propertyValue()
    {
        return randoms.propertyValue();
    }

    // ============================
    // Other utility methods
    // ============================

    public void reset()
    {
        random = new Random( seed );
        randoms = new Randoms( random, config );
    }

    public Randoms fork( Configuration configuration )
    {
        return randoms.fork( configuration );
    }

    public long seed()
    {
        return seed;
    }

    public Random random()
    {
        return random;
    }

    public Randoms randoms()
    {
        return randoms;
    }

    @Retention( RetentionPolicy.RUNTIME )
    @Target( ElementType.METHOD )
    public @interface Seed
    {
        long value();
    }
}
