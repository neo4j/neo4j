/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
    private static final double STANDARD_PROPABILITY_FACTOR = 1.0;
    private final double failureRate;
    private final double errorRate;
    private volatile double probabilityFactor;

    public RandomAdversary( double failureRate, double errorRate )
    {
        assert 0 <= failureRate && failureRate < 1.0 :
                "Expected failure rate in [0.0; 1.0[ but was " + failureRate;
        assert 0 <= errorRate && errorRate < 1.0 :
                "Expected error rate in [0.0; 1.0[ but was " + errorRate;
        assert errorRate + failureRate < 1.0 :
                "Expected error rate + failure rate in [0.0; 1.0[ but was " + (errorRate + failureRate);

        this.failureRate = failureRate;
        this.errorRate = errorRate;
        probabilityFactor = STANDARD_PROPABILITY_FACTOR;
    }

    @Override
    public void injectFailure( Class<? extends Throwable>... failureTypes )
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
                probabilityFactor = STANDARD_PROPABILITY_FACTOR;
            }
            throwOneOf( OutOfMemoryError.class, NullPointerException.class );
        }
        if ( failureTypes.length > 0 && luckyDraw <= (failureRate + errorRate) * factor )
        {
            if ( resetUponFailure )
            {
                probabilityFactor = STANDARD_PROPABILITY_FACTOR;
            }
            throwOneOf( failureTypes );
        }
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
