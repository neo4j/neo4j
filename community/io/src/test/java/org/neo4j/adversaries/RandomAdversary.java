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
package org.neo4j.adversaries;

/**
 * An adversary that injects failures randomly, based on a configured probability.
 */
@SuppressWarnings( "unchecked" )
public class RandomAdversary extends AbstractAdversary
{
    private static final double STANDARD_PROBABILITY_FACTOR = 1.0;
    private final double mischiefRate;
    private final double failureRate;
    private final double errorRate;
    private volatile double probabilityFactor;

    public RandomAdversary( double mischiefRate, double failureRate, double errorRate )
    {
        assert 0 <= mischiefRate && mischiefRate < 1.0 :
                "Expected mischief rate in [0.0; 1.0[ but was " + mischiefRate;
        assert 0 <= failureRate && failureRate < 1.0 :
                "Expected failure rate in [0.0; 1.0[ but was " + failureRate;
        assert 0 <= errorRate && errorRate < 1.0 :
                "Expected error rate in [0.0; 1.0[ but was " + errorRate;
        assert mischiefRate + errorRate + failureRate < 1.0 :
                "Expected error rate + failure rate in [0.0; 1.0[ but was " +
                        (mischiefRate + errorRate + failureRate);

        this.mischiefRate = mischiefRate;
        this.failureRate = failureRate;
        this.errorRate = errorRate;
        probabilityFactor = STANDARD_PROBABILITY_FACTOR;
    }

    @Override
    public void injectFailure( Class<? extends Throwable>... failureTypes )
    {
        maybeDoBadStuff( failureTypes, false );
    }

    @Override
    public boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
    {
        return maybeDoBadStuff( failureTypes, true );
    }

    private boolean maybeDoBadStuff( Class<? extends Throwable>[] failureTypes, boolean includingMischeif )
    {
        double luckyDraw = rng.nextDouble();
        double factor = probabilityFactor;
        boolean resetUponFailure = false;
        if ( factor < 0 )
        {
            resetUponFailure = true;
            factor = -factor;
        }

        if ( luckyDraw <= errorRate * factor )
        {
            if ( resetUponFailure )
            {
                probabilityFactor = STANDARD_PROBABILITY_FACTOR;
            }
            throwOneOf( OutOfMemoryError.class, NullPointerException.class );
        }
        if ( failureTypes.length > 0 && luckyDraw <= (failureRate + errorRate) * factor )
        {
            if ( resetUponFailure )
            {
                probabilityFactor = STANDARD_PROBABILITY_FACTOR;
            }
            throwOneOf( failureTypes );
        }
        return includingMischeif && luckyDraw <= (mischiefRate + failureRate + errorRate) * factor;
    }

    public void setProbabilityFactor( double factor )
    {
        probabilityFactor = factor;
    }

    public void setAndResetProbabilityFactor( double factor )
    {
        // The negative sign bit indicates that the rate should be reset upon failure
        probabilityFactor = -factor;
    }
}
