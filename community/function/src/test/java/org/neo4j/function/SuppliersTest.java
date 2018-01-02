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
package org.neo4j.function;

import org.junit.Test;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SuppliersTest
{
    @Test
    public void singletonSupplierShouldAlwaysReturnSame()
    {
        Object o = new Object();
        Supplier<Object> supplier = Suppliers.singleton( o );

        assertThat( supplier.get(), sameInstance( o ) );
        assertThat( supplier.get(), sameInstance( o ) );
        assertThat( supplier.get(), sameInstance( o ) );
    }

    @Test
    public void lazySingletonSupplierShouldOnlyRequestInstanceWhenRequired()
    {
        Object o = new Object();
        Supplier<Object> mockSupplier = mock( Supplier.class );
        when( mockSupplier.get() ).thenReturn( o );
        Supplier<Object> supplier = Suppliers.lazySingleton( mockSupplier );

        verifyZeroInteractions( mockSupplier );

        assertThat( supplier.get(), sameInstance( o ) );
        assertThat( supplier.get(), sameInstance( o ) );
        assertThat( supplier.get(), sameInstance( o ) );

        verify( mockSupplier ).get();
        verifyNoMoreInteractions( mockSupplier );
    }

    @Test
    public void adapedSupplierShouldOnlyCallAdaptorOnceForEachNewInstance()
    {
        Object o1 = new Object();
        Object o1a = new Object();
        Object o2 = new Object();
        Object o2a = new Object();
        Object o3 = new Object();
        Object o3a = new Object();
        Supplier<Object> mockSupplier = mock( Supplier.class );
        when( mockSupplier.get() ).thenReturn( o1, o1, o1, o2, o3, o3 );

        Function<Object, Object> mockFunction = mock( Function.class );
        when( mockFunction.apply( o1 ) ).thenReturn(o1a);
        when( mockFunction.apply( o2 ) ).thenReturn(o2a);
        when( mockFunction.apply( o3 ) ).thenReturn(o3a);

        Supplier<Object> supplier = Suppliers.adapted( mockSupplier, mockFunction );

        assertThat( supplier.get(), sameInstance( o1a ) );
        assertThat( supplier.get(), sameInstance( o1a ) );
        assertThat( supplier.get(), sameInstance( o1a ) );
        assertThat( supplier.get(), sameInstance( o2a ) );
        assertThat( supplier.get(), sameInstance( o3a ) );
        assertThat( supplier.get(), sameInstance( o3a ) );

        verify( mockFunction ).apply( o1 );
        verify( mockFunction ).apply( o2 );
        verify( mockFunction ).apply( o3 );
        verifyNoMoreInteractions( mockFunction );
    }
}
