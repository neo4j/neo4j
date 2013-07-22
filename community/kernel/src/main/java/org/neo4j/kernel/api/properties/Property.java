/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import org.neo4j.kernel.api.EntityType;
import org.neo4j.kernel.api.exceptions.PropertyNotFoundException;
import org.neo4j.kernel.impl.nioneo.store.PropertyData;

public abstract class Property
{
    public static Property noNodeProperty( long nodeId, long propertyKeyId )
    {
        return noProperty( propertyKeyId, EntityType.NODE, nodeId );
    }

    public static Property noRelationshipProperty( long relationshipId, long propertyKeyId )
    {
        return noProperty( propertyKeyId, EntityType.RELATIONSHIP, relationshipId );
    }
    
    public static Property noGraphProperty( long propertyKeyId )
    {
        return new NoGraphProperty( propertyKeyId );
    }
    
    public static Property noProperty( long propertyKeyId, EntityType type, long entityId )
    {
        return new NoProperty( propertyKeyId, type, entityId );
    }

    public static Property propertyFromNode( long nodeId, long propertyKeyId, Object value )
    {
        return null == value ? noNodeProperty( nodeId, propertyKeyId ) : property( propertyKeyId, value );
    }

    public static Property propertyFromRelationship( long relationshipId, long propertyKeyId, Object value )
    {
        return null == value ?
            noRelationshipProperty( relationshipId, propertyKeyId ) : property( propertyKeyId, value );
    }

    public static Property property( long propertyKeyId, Object value )
    {
        return PropertyConversion.convertProperty( propertyKeyId, value );
    }

    public abstract long propertyKeyId();

    public abstract boolean valueEquals( Object other );

    public abstract Object value() throws PropertyNotFoundException;

    public abstract Object value( Object defaultValue );

    public abstract String stringValue() throws PropertyNotFoundException;

    public abstract String stringValue( String defaultValue );

    public abstract Number numberValue() throws PropertyNotFoundException;

    public abstract Number numberValue( Number defaultValue );

    public abstract int intValue() throws PropertyNotFoundException;

    public abstract int intValue( int defaultValue );

    public abstract long longValue() throws PropertyNotFoundException;

    public abstract long longValue( long defaultValue );

    public abstract boolean booleanValue() throws PropertyNotFoundException;

    public abstract boolean booleanValue( boolean defaultValue );

    // more factory methods

    public static Property stringProperty( long propertyKeyId, String value )
    {
        return new StringProperty( propertyKeyId, value );
    }

    public static Property longProperty( long propertyKeyId, long value )
    {
        return PropertyConversion.chooseLongPropertyType( propertyKeyId, value );
    }

    public static Property intProperty( long propertyKeyId, int value )
    {
        return new IntProperty( propertyKeyId, value );
    }

    public static Property shortProperty( long propertyKeyId, short value )
    {
        return new ShortProperty( propertyKeyId, value );
    }

    public static Property byteProperty( long propertyKeyId, byte value )
    {
        return new ByteProperty( propertyKeyId, value );
    }

    public static Property booleanProperty( long propertyKeyId, boolean value )
    {
        return new BooleanProperty( propertyKeyId, value );
    }

    public static Property charProperty( long propertyKeyId, char value )
    {
        return new CharProperty( propertyKeyId, value );
    }

    public static Property doubleProperty( long propertyKeyId, double value )
    {
        return new DoubleProperty( propertyKeyId, value );
    }

    public static Property floatProperty( long propertyKeyId, float value )
    {
        return new FloatProperty( propertyKeyId, value );
    }

    public static Property stringArrayProperty( long propertyKeyId, String[] value )
    {
        return new StringArrayProperty( propertyKeyId, value );
    }

    public static Property byteArrayProperty( long propertyKeyId, byte[] value )
    {
        return new ByteArrayProperty( propertyKeyId, value );
    }

    public static Property longArrayProperty( long propertyKeyId, long[] value )
    {
        return new LongArrayProperty( propertyKeyId, value );
    }

    public static Property intArrayProperty( long propertyKeyId, int[] value )
    {
        return new IntArrayProperty( propertyKeyId, value );
    }

    public static Property doubleArrayProperty( long propertyKeyId, double[] value )
    {
        return new DoubleArrayProperty( propertyKeyId, value );
    }

    public static Property floatArrayProperty( long propertyKeyId, float[] value )
    {
        return new FloatArrayProperty( propertyKeyId, value );
    }

    public static Property booleanArrayProperty( long propertyKeyId, boolean[] value )
    {
        return new BooleanArrayProperty( propertyKeyId,value );
    }

    public static Property charArrayProperty( long propertyKeyId, char[] value )
    {
        return new CharArrayProperty( propertyKeyId, value );
    }

    public static Property shortArrayProperty( long propertyKeyId, short[] value )
    {
        return new ShortArrayProperty( propertyKeyId, value );
    }

    Property()
    {
    }

    @Override
    public abstract boolean equals( Object obj );

    @Override
    public abstract int hashCode();

    public abstract boolean isNoProperty();
    
    @Deprecated
    public abstract PropertyData asPropertyDataJustForIntegration();
}
