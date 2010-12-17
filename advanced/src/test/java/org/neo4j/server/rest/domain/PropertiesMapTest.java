/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.rest.domain;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.server.rest.domain.PropertiesMap;
import org.neo4j.server.rest.web.*;
import org.neo4j.server.rest.web.PropertyValueException;

public class PropertiesMapTest {

    @Test
    public void shouldContainAddedPropertiesWhenCreatedFromPropertyContainer() {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("foo", "bar");
        PropertiesMap properties = new PropertiesMap(container(values));
        assertEquals("bar", properties.getValue("foo"));
    }

    @Test
    public void shouldContainAddedPropertiesWhenCreatedFromMap() throws org.neo4j.server.rest.web.PropertyValueException
    {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("foo", "bar");
        PropertiesMap properties = new PropertiesMap(values);
        assertEquals("bar", properties.getValue("foo"));
    }

    @Test
    public void shouldSerializeToMapWithSamePropertiesWhenCreatedFromPropertyContainer() {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("foo", "bar");
        PropertiesMap properties = new PropertiesMap(container(values));
        Map<String, Object> map = properties.serialize();
        assertEquals(values, map);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldConvertToNeo4jValueTypesWhenCreatingFromMap() throws PropertyValueException
    {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("string", "value");
        values.put("int", 5);
        values.put("long", 17L);
        values.put("double", 3.14);
        values.put("float", 42.0f);
        values.put("string list", Arrays.asList("one", "two"));
        values.put("long list", Arrays.asList(5, 17L));
        values.put("double list", Arrays.asList(3.14, 42.0f));

        PropertiesMap properties = new PropertiesMap(values);

        assertEquals("value", properties.getValue("string"));
        assertEquals(5, ((Integer) properties.getValue("int")).intValue());
        assertEquals(17L, ((Long) properties.getValue("long")).longValue());
        assertEquals(3.14, ((Double) properties.getValue("double")).doubleValue(), 0);
        assertEquals(42.0f, ((Float) properties.getValue("float")).floatValue(), 0);
        assertArrayEquals(new String[] { "one", "two" }, (String[]) properties.getValue("string list"));
        assertArrayEquals(new Long[] { 5L, 17L }, (Long[]) properties.getValue("long list"));
        assertArrayEquals(new Double[] { 3.14, 42.0 }, (Double[]) properties.getValue("double list"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldSerializeToMap() {
        Map<String, Object> values = new HashMap<String, Object>();
        values.put("string", "value");
        values.put("int", 5);
        values.put("long", 17L);
        values.put("double", 3.14);
        values.put("float", 42.0f);
        values.put("string array", new String[] { "one", "two" });
        values.put("long array", new long[] { 5L, 17L });
        values.put("double array", new double[] { 3.14, 42.0 });

        PropertiesMap properties = new PropertiesMap(container(values));
        Map<String, Object> map = properties.serialize();

        assertEquals("value", map.get("string"));
        assertEquals(5, map.get("int"));
        assertEquals(17L, map.get("long"));
        assertEquals(3.14, map.get("double"));
        assertEquals(42.0f, map.get("float"));
        assertEqualContent(Arrays.asList("one", "two"), (List) map.get("string array"));
        assertEqualContent(Arrays.asList(5L, 17L), (List) map.get("long array"));
        assertEqualContent(Arrays.asList(3.14, 42.0), (List) map.get("double array"));
    }

    @Test
    public void shouldBeAbleToSignalEmptiness() throws PropertyValueException
    {
        Map<String, Object> values = new HashMap<String, Object>();
        PropertiesMap properties = new PropertiesMap(values);
        values.put("key", "value");
        assertTrue(properties.isEmpty());
        properties = new PropertiesMap(values);
        assertFalse(properties.isEmpty());
    }

    private void assertEqualContent(List<?> expected, List<?> actual) {
        assertEquals(expected.size(), actual.size());
        for (Iterator<?> ex = expected.iterator(), ac = actual.iterator(); ex.hasNext() && ac.hasNext();) {
            assertEquals(ex.next(), ac.next());
        }
    }

    private PropertyContainer container(Map<String, Object> values) {
        PropertyContainer container = mock(PropertyContainer.class);
        when(container.getPropertyKeys()).thenReturn(values.keySet());
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            when(container.getProperty(entry.getKey())).thenReturn(entry.getValue());
        }
        return container;
    }

}
