/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.helper;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompositeSuspendableTest
{

    @Test
    public void shouldEnableAllAndDisableAllEvenIfTheyThrow()
    {
        AtomicInteger count = new AtomicInteger();
        CompositeSuspendable compositeSuspendable = new CompositeSuspendable();
        int amountOfSuspendable = 3;
        for ( int i = 0; i < amountOfSuspendable; i++ )
        {
            compositeSuspendable.add( getSuspendable( count ) );
        }

        try
        {
            compositeSuspendable.enable();
            fail();
        }
        catch ( RuntimeException ignore )
        {

        }

        assertEquals( amountOfSuspendable, count.get() );

        try
        {
            compositeSuspendable.disable();
            fail();
        }
        catch ( RuntimeException ignore )
        {

        }

        assertEquals( 0, count.get() );
    }

    private Suspendable getSuspendable( AtomicInteger count )
    {
        return new Suspendable()
        {
            @Override
            public void enable()
            {
                count.incrementAndGet();
                fail();
            }

            @Override
            public void disable()
            {
                count.decrementAndGet();
                fail();
            }
        };
    }
}
