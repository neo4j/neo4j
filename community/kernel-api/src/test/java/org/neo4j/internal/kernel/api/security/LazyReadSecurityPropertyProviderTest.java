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
package org.neo4j.internal.kernel.api.security;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.eclipse.collections.api.map.primitive.IntObjectMap;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

class LazyReadSecurityPropertyProviderTest {

    @Test
    void testGetSecurityPropertiesWhenNoProperties() {
        StoragePropertyCursor securityPropCursor = mock(StoragePropertyCursor.class);
        when(securityPropCursor.next()).thenReturn(false);

        var provider = new ReadSecurityPropertyProvider.LazyReadSecurityPropertyProvider(securityPropCursor);
        IntObjectMap<Value> properties = provider.getSecurityProperties();
        Assertions.assertEquals(0, properties.size());
    }

    @Test
    void testGetSecurityPropertiesWhenOneProperty() {
        StoragePropertyCursor securityPropCursor = mock(StoragePropertyCursor.class);
        when(securityPropCursor.next()).thenReturn(true).thenReturn(false);
        when(securityPropCursor.propertyKey()).thenReturn(1);
        when(securityPropCursor.propertyValue()).thenReturn(Values.intValue(1));

        var provider = new ReadSecurityPropertyProvider.LazyReadSecurityPropertyProvider(securityPropCursor);
        IntObjectMap<Value> properties = provider.getSecurityProperties();
        Assertions.assertEquals(1, properties.size());
        Assertions.assertEquals(Values.intValue(1), properties.get(1));
    }

    @Test
    void testGetSecurityPropertiesWhenMultipleProperties() {
        StoragePropertyCursor securityPropCursor = mock(StoragePropertyCursor.class);
        when(securityPropCursor.next()).thenReturn(true, true, false);
        when(securityPropCursor.propertyKey()).thenReturn(1, 2);
        when(securityPropCursor.propertyValue()).thenReturn(Values.intValue(1), Values.intValue(2));

        var provider = new ReadSecurityPropertyProvider.LazyReadSecurityPropertyProvider(securityPropCursor);
        IntObjectMap<Value> properties = provider.getSecurityProperties();
        Assertions.assertEquals(2, properties.size());
        Assertions.assertEquals(Values.intValue(1), properties.get(1));
        Assertions.assertEquals(Values.intValue(2), properties.get(2));
    }

    @Test
    void testGetSecurityPropertiesOnlyCallsCursorOnce() {
        StoragePropertyCursor securityPropCursor = mock(StoragePropertyCursor.class);
        Mockito.when(securityPropCursor.next()).thenReturn(true).thenReturn(false);
        Mockito.when(securityPropCursor.propertyKey()).thenReturn(1);
        Mockito.when(securityPropCursor.propertyValue()).thenReturn(Values.intValue(1));

        var provider = new ReadSecurityPropertyProvider.LazyReadSecurityPropertyProvider(securityPropCursor);
        IntObjectMap<Value> properties1 = provider.getSecurityProperties();
        // Assert that the cursor is only called two times (once to get the value, then once to signal exhaustion).
        Mockito.verify(securityPropCursor, Mockito.times(2)).next();
        IntObjectMap<Value> properties2 = provider.getSecurityProperties();
        // Assert that the cursor has not been called again (call count still at 2) to show that the cached properties
        // are used.
        Mockito.verify(securityPropCursor, Mockito.times(2)).next();
        Assertions.assertEquals(properties1, properties2);
    }
}
