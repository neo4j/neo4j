/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.api.properties;

import java.util.concurrent.Callable;

import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;

public abstract class Property
{
    public static Property noNodeProperty( long nodeId, int propertyKeyId )
    {
        return noProperty( propertyKeyId, EntityType.NODE, nodeId );
    }

    public static Property noRelationshipProperty( long relationshipId, int propertyKeyId )
    {
        return noProperty( propertyKeyId, EntityType.RELATIONSHIP, relationshipId );
    }

    public static Property noGraphProperty( int propertyKeyId )
    {
        return noProperty( propertyKeyId, EntityType.GRAPH, -1 );
    }

    private static Property noProperty( int propertyKeyId, EntityType type, long entityId )
    {
        return new NoProperty( propertyKeyId, type, entityId );
    }

    public static DefinedProperty property( int propertyKeyId, Object value )
    {
        return PropertyConversion.convertProperty( propertyKeyId, value );
    }

    final int propertyKeyId;

    Property( int propertyKeyId )
    {
        this.propertyKeyId = propertyKeyId;
    }

    public final int propertyKeyId()
    {
        return propertyKeyId;
    }

    public abstract boolean valueEquals( Object other );

    public abstract Object value() throws PropertyNotFoundException;

    public abstract Object value( Object defaultValue );

    public abstract String valueAsString() throws PropertyNotFoundException;

    @Override
    public abstract boolean equals( Object obj );

    @Override
    public abstract int hashCode();

    public abstract boolean isDefined();

    // direct factory methods

    public static DefinedProperty stringProperty( int propertyKeyId, String value )
    {
        return new StringProperty( propertyKeyId, value );
    }

    public static DefinedProperty lazyStringProperty( int propertyKeyId, Callable<String> producer )
    {
        return new LazyStringProperty( propertyKeyId, producer );
    }

    public static DefinedProperty lazyArrayProperty( int propertyKeyId, Callable<Object> producer )
    {
        return new LazyArrayProperty( propertyKeyId, producer );
    }

    public static DefinedProperty longProperty( int propertyKeyId, long value )
    {
        return new LongProperty( propertyKeyId, value );
    }

    public static DefinedProperty intProperty( int propertyKeyId, int value )
    {
        return new IntProperty( propertyKeyId, value );
    }

    public static DefinedProperty shortProperty( int propertyKeyId, short value )
    {
        return new ShortProperty( propertyKeyId, value );
    }

    public static DefinedProperty byteProperty( int propertyKeyId, byte value )
    {
        return new ByteProperty( propertyKeyId, value );
    }

    public static DefinedProperty booleanProperty( int propertyKeyId, boolean value )
    {
        return new BooleanProperty( propertyKeyId, value );
    }

    public static DefinedProperty charProperty( int propertyKeyId, char value )
    {
        return new CharProperty( propertyKeyId, value );
    }

    public static DefinedProperty doubleProperty( int propertyKeyId, double value )
    {
        return new DoubleProperty( propertyKeyId, value );
    }

    public static DefinedProperty floatProperty( int propertyKeyId, float value )
    {
        return new FloatProperty( propertyKeyId, value );
    }

    public static DefinedProperty stringArrayProperty( int propertyKeyId, String[] value )
    {
        return new StringArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty byteArrayProperty( int propertyKeyId, byte[] value )
    {
        return new ByteArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty longArrayProperty( int propertyKeyId, long[] value )
    {
        return new LongArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty intArrayProperty( int propertyKeyId, int[] value )
    {
        return new IntArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty doubleArrayProperty( int propertyKeyId, double[] value )
    {
        return new DoubleArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty floatArrayProperty( int propertyKeyId, float[] value )
    {
        return new FloatArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty booleanArrayProperty( int propertyKeyId, boolean[] value )
    {
        return new BooleanArrayProperty( propertyKeyId,value );
    }

    public static DefinedProperty charArrayProperty( int propertyKeyId, char[] value )
    {
        return new CharArrayProperty( propertyKeyId, value );
    }

    public static DefinedProperty shortArrayProperty( int propertyKeyId, short[] value )
    {
        return new ShortArrayProperty( propertyKeyId, value );
    }
}
