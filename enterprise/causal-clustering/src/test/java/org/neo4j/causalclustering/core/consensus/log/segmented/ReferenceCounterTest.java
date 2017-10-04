/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
    public void shouldHaveValidInitialBehaviour() throws Exception
    {
        assertEquals( 0, refCount.get() );
        assertTrue( refCount.tryDispose() );
    }

    @Test
    public void shouldNotBeAbleToDisposeWhenActive() throws Exception
    {
        // when
        refCount.increase();

        // then
        assertFalse( refCount.tryDispose() );
    }

    @Test
    public void shouldBeAbleToDisposeInactive() throws Exception
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
    public void shouldNotGiveReferenceWhenDisposed() throws Exception
    {
        // given
        refCount.tryDispose();

        // then
        assertFalse( refCount.increase() );
    }

    @Test
    public void shouldAdjustCounterWithReferences() throws Exception
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
    public void shouldThrowIllegalStateExceptionWhenDecreasingPastZero() throws Exception
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
    public void shouldThrowIllegalStateExceptionWhenDecreasingOnDisposed() throws Exception
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
