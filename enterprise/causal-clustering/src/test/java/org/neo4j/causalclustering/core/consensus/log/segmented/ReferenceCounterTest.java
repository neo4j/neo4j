/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReferenceCounterTest
{
    private ReferenceCounter refCount = new ReferenceCounter();

    @Test
    public void shouldHaveValidInitialBehaviour()
    {
        assertEquals( 0, refCount.get() );
        assertTrue( refCount.tryDispose() );
    }

    @Test
    public void shouldNotBeAbleToDisposeWhenActive()
    {
        // when
        refCount.increase();

        // then
        assertFalse( refCount.tryDispose() );
    }

    @Test
    public void shouldBeAbleToDisposeInactive()
    {
        // given
        refCount.increase();
        refCount.increase();

        // when / then
        refCount.decrease();
        assertFalse( refCount.tryDispose() );

        // when / then
        refCount.decrease();
        assertTrue( refCount.tryDispose() );
    }

    @Test
    public void shouldNotGiveReferenceWhenDisposed()
    {
        // given
        refCount.tryDispose();

        // then
        assertFalse( refCount.increase() );
    }

    @Test
    public void shouldAdjustCounterWithReferences()
    {
        // when / then
        refCount.increase();
        assertEquals( 1, refCount.get() );

        // when / then
        refCount.increase();
        assertEquals( 2, refCount.get() );

        // when / then
        refCount.decrease();
        assertEquals( 1, refCount.get() );

        // when / then
        refCount.decrease();
        assertEquals( 0, refCount.get() );
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenDecreasingPastZero()
    {
        // given
        refCount.increase();
        refCount.decrease();

        // when
        try
        {
            refCount.decrease();
            fail();
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }

    @Test
    public void shouldThrowIllegalStateExceptionWhenDecreasingOnDisposed()
    {
        // given
        refCount.tryDispose();

        // when
        try
        {
            refCount.decrease();
            fail();
        }
        catch ( IllegalStateException e )
        {
            // expected
        }
    }
}
