/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.ha;

import org.junit.Test;

import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientDatabaseFailureException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DelegateInvocationHandlerTest
{
    @Test
    public void shouldNotBeAbleToUseValueBeforeHardened()
    {
        // GIVEN
        DelegateInvocationHandler<Value> handler = newDelegateInvocationHandler();
        Value value = handler.cement();

        // WHEN
        try
        {
            value.get();
            fail( "Should fail" );
        }
        catch ( Exception e )
        {
            // THEN
            assertThat( e, instanceOf( TransientDatabaseFailureException.class ) );
        }
    }

    @Test
    public void throwsWhenDelegateIsNotSet()
    {
        DelegateInvocationHandler<Value> handler = newDelegateInvocationHandler();

        try
        {
            handler.invoke( new Object(), Value.class.getDeclaredMethod( "get" ), new Object[0] );
            fail( "Exception expected" );
        }
        catch ( Throwable t )
        {
            assertThat( t, instanceOf( TransactionFailureException.class ) );
        }
    }

    @Test
    public void shouldBeAbleToUseCementedValueOnceDelegateSet()
    {
        // GIVEN
        DelegateInvocationHandler<Value> handler = newDelegateInvocationHandler();
        Value cementedValue = handler.cement();

        // WHEN setting the delegate (implies hardening it)
        handler.setDelegate( value( 10 ) );

        // THEN
        assertEquals( 10, cementedValue.get() );
    }

    @Test
    public void shouldBeAbleToUseCementedValueOnceHardened()
    {
        // GIVEN
        DelegateInvocationHandler<Value> handler = newDelegateInvocationHandler();
        Value cementedValue = handler.cement();

        // WHEN setting the delegate (implies hardening it)
        handler.setDelegate( value( 10 ) );

        // THEN
        assertEquals( 10, cementedValue.get() );
    }

    @Test
    public void setDelegateShouldBeAbleToOverridePreviousHarden()
    {
        /* This test case stems from a race condition where a thread switching role to slave and
         * HaKernelPanicHandler thread were competing to harden the master delegate handler.
         * While all components were switching to their slave versions a kernel panic event came
         * in and made its switch, ending with hardening the master delegate handler. All components
         * would take that as a sign about the master delegate being ready to use. When the slave switching
         * was done, that thread would set the master delegate to the actual one, but all components would
         * have already gotten a concrete reference to the master such that the latter setDelegate call would
         * not affect them. */

        // GIVEN
        DelegateInvocationHandler<Value> handler = newDelegateInvocationHandler();
        handler.setDelegate( value( 10 ) );
        Value cementedValue = handler.cement();
        handler.harden();

        // WHEN setting the delegate (implies hardening it)
        handler.setDelegate( value( 20 ) );

        // THEN
        assertEquals( 20, cementedValue.get() );
    }

    private Value value( final int i )
    {
        return () -> i;
    }

    private static DelegateInvocationHandler<Value> newDelegateInvocationHandler()
    {
        return new DelegateInvocationHandler<>( Value.class );
    }

    private interface Value
    {
        int get();
    }
}
