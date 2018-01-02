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
package org.neo4j.visualization;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public enum PropertyType
{
	/**
	 * Represents a String property.
	 */
	STRING( null, "String", String.class )
	{
		@Override
		<T> T apply( ValueFormatter<T> formatter, Object value )
		{
			return formatter.formatString( ( String ) value );
		}
	},
	/**
	 * Represents an integer property.
	 */
	INT( null, "int", Integer.class, int.class ),
	/**
	 * Represents a long property.
	 */
	LONG( null, "long", Long.class, long.class ),
	/**
	 * Represents a boolean property.
	 */
	BOOLEAN( null, "boolean", Boolean.class, boolean.class ),
	/**
	 * Represents a short property.
	 */
	SHORT( null, "short", Short.class, short.class ),
	/**
	 * Represents a character property.
	 */
	CHAR( null, "char", Character.class, char.class ),
	/**
	 * Represents a byte property.
	 */
	BYTE( null, "byte", Byte.class, byte.class ),
	/**
	 * Represents a float property.
	 */
	FLOAT( null, "float", Float.class, float.class ),
	/**
	 * Represents a double property.
	 */
	DOUBLE( null, "double", Double.class, double.class ),
	// Array types
	/**
	 * Represents an array of Strings.
	 */
	STRING_ARRAY( String.class, "String[]", String[].class )
	{
		@Override
		<T> T apply( ValueFormatter<T> formatter, Object value )
		{
			return formatter.formatStringArray( ( String[] ) value );
		}
	},
	/**
	 * Represents an array of integers.
	 */
	INT_ARRAY( Integer.class, "int[]", Integer[].class, int[].class ),
	/**
	 * Represents an array of longs.
	 */
	LONG_ARRAY( Long.class, "long[]", Long[].class, long[].class ),
	/**
	 * Represents an array of booleans.
	 */
	BOOLEAN_ARRAY( Boolean.class, "boolean[]", Boolean[].class, boolean[].class ),
	/**
	 * Represents an array of shorts.
	 */
	SHORT_ARRAY( Short.class, "short[]", Short[].class, short[].class ),
	/**
	 * Represents an array of characters.
	 */
	CHAR_ARRAY( Character.class, "char[]", Character[].class, char[].class ),
	/**
	 * Represents an array of bytes.
	 */
	BYTE_ARRAY( Byte.class, "byte[]", Byte[].class, byte[].class ),
	/**
	 * Represents an array of floats.
	 */
	FLOAT_ARRAY( Float.class, "float[]", Float[].class, float[].class ),
	/**
	 * Represents an array of doubles.
	 */
	DOUBLE_ARRAY( Double.class, "double[]", Double[].class, double[].class ),
	/**
	 * Represents an undefined type.
	 */
	UNDEFINED( null, "Object" )
	{
		@Override
		<T> T apply( ValueFormatter<T> formatter, Object value )
		{
			return formatter.formatUnknownObject( value );
		}
	};
	/**
	 * Get the {@link PropertyType} representing the type of a value.
	 * @param propertyValue
	 *            the value to get the type of.
	 * @return the type of the given value.
	 */
	public static PropertyType getTypeOf( Object propertyValue )
	{
		return getTypeFor( propertyValue.getClass() );
	}

	static PropertyType getTypeFor( Class<? extends Object> type )
	{
		PropertyType result = typeMap.get( type );
		if ( result != null )
		{
			return result;
		}
		else
		{
			return UNDEFINED;
		}
	}

	<T> T apply( ValueFormatter<T> formatter, Object value )
	{
		if ( scalarType != null )
		{
			PropertyType type = getTypeFor( scalarType );
			if ( value instanceof Object[] )
			{
				return formatter.formatBoxedPrimitiveArray( type,
				    ( Object[] ) value );
			}
			else
			{
				return formatter.formatPrimitiveArray( type, value );
			}
		}
		else
		{
			return formatter.formatBoxedPrimitive( this, value );
		}
	}

	/**
	 * Apply a formatter to a value and return the result.
	 * @param <T>
	 *            the type of the result.
	 * @param formatter
	 *            the formatter to apply to the value.
	 * @param propertyValue
	 *            the value to apply the formatter to.
	 * @return the value as produced by the formatter.
	 */
	public static <T> T format( ValueFormatter<T> formatter,
	    Object propertyValue )
	{
		return getTypeOf( propertyValue ).apply( formatter, propertyValue );
	}

	/**
	 * Format a given value to a String by applying a string formatter.
	 * @see #format(ValueFormatter, Object)
	 * @param propertyValue
	 *            the value to turn into a string.
	 * @return the given value formatted as a string.
	 */
	public static String format( Object propertyValue )
	{
		return format( ValueFormatter.DEFAULT_STRING_FORMATTER, propertyValue );
	}

	private final Class<?>[] types;
	private final Class<?> scalarType;
	/**
	 * Specifies the name of the type.
	 */
	public final String typeName;

	private PropertyType( Class<?> scalarType, String descriptor,
	    Class<?>... types )
	{
		this.typeName = descriptor;
		this.types = types;
		this.scalarType = scalarType;
	}

	private static final Map<Class<?>, PropertyType> typeMap;
	static
	{
		Map<Class<?>, PropertyType> types = new HashMap<Class<?>, PropertyType>();
		for ( PropertyType type : values() )
		{
			for ( Class<?> cls : type.types )
			{
				types.put( cls, type );
			}
		}
		typeMap = Collections.unmodifiableMap( types );
	}
}
