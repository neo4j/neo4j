/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class SuppliersTest {
    @Test
    void singletonSupplierShouldAlwaysReturnSame() {
        Object o = new Object();
        Supplier<Object> supplier = Suppliers.singleton(o);

        assertThat(supplier.get()).isSameAs(o);
        assertThat(supplier.get()).isSameAs(o);
        assertThat(supplier.get()).isSameAs(o);
    }

    @Test
    void lazySingletonSupplierShouldOnlyRequestInstanceWhenRequired() {
        Object o = new Object();
        Supplier<Object> mockSupplier = mock(Supplier.class);
        when(mockSupplier.get()).thenReturn(o);
        Supplier<Object> supplier = Suppliers.lazySingleton(mockSupplier);

        verifyNoInteractions(mockSupplier);

        assertThat(supplier.get()).isSameAs(o);
        assertThat(supplier.get()).isSameAs(o);
        assertThat(supplier.get()).isSameAs(o);

        verify(mockSupplier).get();
        verifyNoMoreInteractions(mockSupplier);
    }

    @Test
    void adaptedSupplierShouldOnlyCallAdaptorOnceForEachNewInstance() {
        Object o1 = new Object();
        Object o1a = new Object();
        Object o2 = new Object();
        Object o2a = new Object();
        Object o3 = new Object();
        Object o3a = new Object();
        Supplier<Object> mockSupplier = mock(Supplier.class);
        when(mockSupplier.get()).thenReturn(o1, o1, o1, o2, o3, o3);

        Function<Object, Object> mockFunction = mock(Function.class);
        when(mockFunction.apply(o1)).thenReturn(o1a);
        when(mockFunction.apply(o2)).thenReturn(o2a);
        when(mockFunction.apply(o3)).thenReturn(o3a);

        Supplier<Object> supplier = Suppliers.adapted(mockSupplier, mockFunction);

        assertThat(supplier.get()).isSameAs(o1a);
        assertThat(supplier.get()).isSameAs(o1a);
        assertThat(supplier.get()).isSameAs(o1a);
        assertThat(supplier.get()).isSameAs(o2a);
        assertThat(supplier.get()).isSameAs(o3a);
        assertThat(supplier.get()).isSameAs(o3a);

        verify(mockFunction).apply(o1);
        verify(mockFunction).apply(o2);
        verify(mockFunction).apply(o3);
        verifyNoMoreInteractions(mockFunction);
    }

    @Test
    void correctlyReportNotInitialisedSuppliers() {
        Suppliers.Lazy<Object> lazySingleton = Suppliers.lazySingleton(() -> "a");
        assertFalse(lazySingleton.isInitialised());
        assertNull(lazySingleton.getIfPresent());
        assertEquals("null", lazySingleton.toString());
    }

    @Test
    void correctlyReportInitialisedSuppliers() {
        Suppliers.Lazy<Object> lazySingleton = Suppliers.lazySingleton(() -> "a");
        assertNotNull(lazySingleton.get());
        assertTrue(lazySingleton.isInitialised());
        assertNotNull(lazySingleton.getIfPresent());
        assertEquals("a", lazySingleton.toString());
    }
}
