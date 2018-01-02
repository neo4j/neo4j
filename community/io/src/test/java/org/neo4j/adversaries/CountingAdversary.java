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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * An adversary that inject failures after a configured count of invocations.
 */
public class CountingAdversary extends AbstractAdversary
{
    private final AtomicInteger countDown;
    private final int startingCount;
    private final boolean resetCountDownOnFailure;

    public CountingAdversary( int countDownTillFailure, boolean resetCountDownOnFailure )
    {
        this.startingCount = countDownTillFailure;
        this.resetCountDownOnFailure = resetCountDownOnFailure;
        countDown = new AtomicInteger( countDownTillFailure );
    }

    @Override
    public void injectFailure( Class<? extends Throwable>... failureTypes )
    {
        int count, newCount;
        do {
            count = countDown.get();
            newCount = count - 1;
        } while( !countDown.compareAndSet( count, newCount ) );

        if ( resetCountDownOnFailure && newCount < 1 )
        {
            reset();
        }

        if ( newCount == 0 )
        {
            try
            {
                Thread.sleep( 10 );
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
            }
            throwOneOf( failureTypes );
        }
    }

    @Override
    public boolean injectFailureOrMischief( Class<? extends Throwable>... failureTypes )
    {
        injectFailure( failureTypes );
        return false;
    }

    private void reset()
    {
        // The current count is going to be either zero or negative when we get here.
        int count;
        do {
            count = countDown.get();
        } while( count < 1 && !countDown.compareAndSet( count, startingCount + count ) );
    }
}
