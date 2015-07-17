/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.neo4j.graphdb.TransientFailureException;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DelegateInvocationHandlerTest
{
    @Test
    public void shouldNotBeAbleToUseValueBeforeHardened() throws Exception
    {
        // GIVEN
        DelegateInvocationHandler<Value> handler = new DelegateInvocationHandler<Value>( Value.class );
        Value value = handler.cement();
        
        // WHEN
        try
        {
            value.get();
            fail( "Should fail" );
        }
        catch ( TransientFailureException e )
        {   // THEN
        }
    }
    
    @Test
    public void shouldBeAbleToUseCementedValueOnceDelegateSet() throws Exception
    {
        // GIVEN
        DelegateInvocationHandler<Value> handler = new DelegateInvocationHandler<Value>( Value.class );
        Value cementedValue = handler.cement();
        
        // WHEN setting the delegate (implies hardening it)
        handler.setDelegate( value( 10 ) );
        
        // THEN
        assertEquals( 10, cementedValue.get() );
    }
    
    @Test
    public void shouldBeAbleToUseCementedValueOnceHardened() throws Exception
    {
        // GIVEN
        DelegateInvocationHandler<Value> handler = new DelegateInvocationHandler<Value>( Value.class );
        Value cementedValue = handler.cement();
        
        // WHEN setting the delegate (implies hardening it)
        handler.setDelegate( value( 10 ) );
        
        // THEN
        assertEquals( 10, cementedValue.get() );
    }
    
    @Test
    public void setDelegateShouldBeAbleToOverridePreviousHarden() throws Exception
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
        DelegateInvocationHandler<Value> handler = new DelegateInvocationHandler<Value>( Value.class );
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
        return new Value()
        {
            @Override
            public int get()
            {
                return i;
            }
        };
    }

    private interface Value
    {
        int get();
    }
}
