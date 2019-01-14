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
package org.neo4j.jmx.impl;

import org.junit.Test;

import java.time.Clock;

import static org.junit.Assert.assertSame;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.internal.verification.VerificationModeFactory.times;

public class ThrottlingBeanSnapshotProxyTest
{
    private final Clock clock = mock( Clock.class );
    private final TestBean target = mock( TestBean.class );
    private final TestBean proxy = ThrottlingBeanSnapshotProxy.newThrottlingBeanSnapshotProxy( TestBean.class, target, 100, clock );

    @Test
    public void doNotProxyIfUpdateIntervalIsZero()
    {
        final TestBean result = ThrottlingBeanSnapshotProxy.newThrottlingBeanSnapshotProxy( TestBean.class, target, 0, clock );
        assertSame( target, result );
    }

    @Test
    public void throttleGetterInvocation()
    {
        when( clock.millis() ).thenReturn( 100L );
        proxy.getLong();
        proxy.getLong();
        verify( target, times( 1 ) ).getLong();

        when( clock.millis() ).thenReturn( 199L );
        proxy.getLong();
        verify( target, times( 1 ) ).getLong();

        when( clock.millis() ).thenReturn( 200L );
        proxy.getLong();
        proxy.getLong();
        verify( target, times( 2 ) ).getLong();
        verifyNoMoreInteractions( target );
    }

    @Test
    public void dontThrottleMethodsReturningVoid()
    {
        when( clock.millis() ).thenReturn( 100L );
        proxy.returnVoid();
        proxy.returnVoid();
        verify( target, times( 2 ) ).returnVoid();
        verifyNoMoreInteractions( target );
    }

    @Test
    public void dontThrottleMethodsWithArgs()
    {
        when( clock.millis() ).thenReturn( 100L );
        proxy.notGetter( 1 );
        proxy.notGetter( 2 );
        verify( target, times( 2 ) ).notGetter( anyLong() );
        verifyNoMoreInteractions( target );
    }

    private interface TestBean
    {

        void returnVoid();

        long getLong();

        long notGetter( long x );
    }
}
