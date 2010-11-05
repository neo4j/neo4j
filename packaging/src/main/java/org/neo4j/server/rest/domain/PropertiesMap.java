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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.neo4j.graphdb.PropertyContainer;

public class PropertiesMap implements Representation
{

    private final Map<String, Object> values = new HashMap<String, Object>();

    public PropertiesMap(PropertyContainer container) {
        for (String key : container.getPropertyKeys()) {
            values.put(key, container.getProperty(key));
        }
    }

    public PropertiesMap(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            values.put(entry.getKey(), toInternalType(entry.getValue()));
        }
    }

    public Object getValue(String key) {
        return values.get(key);
    }

    public Map<String, Object> serialize() {
        // TODO Nice with sorted, but TreeMap the best?
        Map<String, Object> result = new TreeMap<String, Object>();
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            result.put(entry.getKey(), toSerializedType(entry.getValue()));
        }
        return result;
    }

    void storeTo(PropertyContainer container) {
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            container.setProperty(entry.getKey(), entry.getValue());
        }
    }

    @SuppressWarnings("unchecked")
    private static Object toInternalType(Object value) {
        if (value instanceof List) {
            List list = (List) value;
            if (list.isEmpty()) {
                return new byte[0];
            } else {
                Object first = list.get(0);
                if (first instanceof String) {
                    return stringArray(list);
                } else if (first instanceof Number) {
                    return numberArray(list);
                } else if (first instanceof Boolean) {
                    return booleanArray(list);
                } else {
                    throw new PropertyValueException("Unsupported array type " + first.getClass() +
                            ". Supported array types are arrays of all java primitives (" +
                            "byte[], char[], short[], int[], long[], float[], double[]) " +
                            "and String[]" );
                }
            }
        } else {
            return assertSupportedPropertyValue( value );
        }
    }

    public static Object assertSupportedPropertyValue( Object value )
    {
        if ( value == null )
        {
            throw new PropertyValueException( "null value not supported" );
        }

        if (value instanceof String) {
        } else if (value instanceof Number) {
        } else if (value instanceof Boolean) {
        } else {
            throw new PropertyValueException("Unsupported value type " + value.getClass() + "." +
                    " Supported value types are all java primitives (byte, char, short, int, " +
                    "long, float, double) and String, as well as arrays of all those types" );
        }
        return value;
    }

    private static Boolean[] booleanArray( List<Boolean> list )
    {
        return list.toArray( new Boolean[list.size()] );
    }

    private static Number[] numberArray(List<Number> numbers) {
        Number[] internal = new Number[numbers.size()];
        for (int i = 0; i < internal.length; i++) {
            Number number = numbers.get(i);
            if (number instanceof Float || number instanceof Double) {
                number = number.doubleValue();
            } else {
                number = number.longValue();
            }
            internal[i] = number;
        }
        final Number[] result;
        if (internal[0] instanceof Double) {
            result = new Double[internal.length];
        } else {
            result = new Long[internal.length];
        }
        System.arraycopy( internal, 0, result, 0, internal.length );
        return result;
    }

    private static String[] stringArray(List<String> strings) {
        return strings.toArray(new String[strings.size()]);
    }

    private Object toSerializedType(Object value) {
        if (value.getClass().isArray()) {
            if (value.getClass().getComponentType().isPrimitive()) {
                int size = Array.getLength(value);
                List<Object> result = new ArrayList<Object>();
                for (int i = 0; i < size; i++) {
                    result.add(Array.get(value, i));
                }
                return result;
            } else {
                return Arrays.asList((Object[]) value);
            }
        } else {
            return value;
        }
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }
}
