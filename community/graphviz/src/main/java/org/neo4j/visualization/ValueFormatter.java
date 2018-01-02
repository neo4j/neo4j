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

import java.util.Arrays;

/**
 * A formatter that, directed by a {@link PropertyType} can turn a value into a
 * specific result type.
 * @param <T>
 *            The result type that this formatter yields.
 */
public interface ValueFormatter<T>
{
	/**
	 * Format a string.
	 * @param value
	 *            the string to format.
	 * @return a formatted version of the given string.
	 */
	T formatString( String value );

	/**
	 * Format a string array.
	 * @param array
	 *            the string array to format.
	 * @return a formatted version of the given string array.
	 */
	T formatStringArray( String[] array );

	/**
	 * Format a boxed primitive.
	 * @param type
	 *            an object representing the type of the primitive.
	 * @param value
	 *            the boxed primitive object to format.
	 * @return a formatted version of the given value.
	 */
	T formatBoxedPrimitive( PropertyType type, Object value );

	/**
	 * Format an array of primitives.
	 * @param elementType
	 *            an object representing the type of the elements of the array.
	 * @param array
	 *            the array to format.
	 * @return a formatted version of the given array.
	 */
	T formatPrimitiveArray( PropertyType elementType, Object array );

	/**
	 * Format an array of boxed primitives.
	 * @param elementType
	 *            an object representing the type of the elements of the array.
	 * @param array
	 *            the array to format.
	 * @return a formatted version of the given array.
	 */
	T formatBoxedPrimitiveArray( PropertyType elementType, Object[] array );

	/**
	 * Format an object of unsupported type.
	 * @param value the value of unsupported type
	 * @return a formatted version of the given value.
	 */
	T formatUnknownObject( Object value );

	/**
	 * A default implementation that formats a String.
	 */
	static final ValueFormatter<String> DEFAULT_STRING_FORMATTER = new ValueFormatter<String>()
	{
		public String formatString( String string )
		{
			string = string.replace( "\\n", "\\\\n" );
			string = string.replace( "\\", "\\\\" );
			string = string.replace( "\"", "\\\"" );
			string = string.replace( "'", "\\\\'" );
			string = string.replace( "\n", "\\\\n" );
	        string = string.replace( "<", "\\<" );
	        string = string.replace( ">", "\\>" );
            string = string.replace( "[", "\\[" );
            string = string.replace( "]", "\\]" );
            string = string.replace( "{", "\\{" );
            string = string.replace( "}", "\\}" );
            string = string.replace( "|", "\\|" );

			return "'" + string + "'";
		}

		public String formatStringArray( String[] value )
		{
			boolean comma = false;
			StringBuilder result = new StringBuilder( "[" );
			for ( String string : value )
			{
				if ( comma )
				{
					result.append( ", " );
				}
				result.append( formatString( string ) );
				comma = true;
			}
			result.append( "]" );
			return result.toString();
		}

		public String formatBoxedPrimitive( PropertyType type, Object primitive )
		{
			return primitive.toString();
		}

		public String formatBoxedPrimitiveArray( PropertyType elementType,
		    Object[] array )
		{
			return Arrays.toString( array );
		}

		public String formatPrimitiveArray( PropertyType type, Object array )
		{
			switch ( type )
			{
				case INT:
					return Arrays.toString( ( int[] ) array );
				case LONG:
					return Arrays.toString( ( long[] ) array );
				case BOOLEAN:
					return Arrays.toString( ( boolean[] ) array );
				case SHORT:
					return Arrays.toString( ( short[] ) array );
				case CHAR:
					return Arrays.toString( ( char[] ) array );
				case BYTE:
					return Arrays.toString( ( byte[] ) array );
				case FLOAT:
					return Arrays.toString( ( float[] ) array );
				case DOUBLE:
					return Arrays.toString( ( double[] ) array );
				default:
					throw new IllegalArgumentException();
			}
		}

		public String formatUnknownObject( Object value )
		{
			return value.toString();
		}
	};
}
