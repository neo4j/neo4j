/*
 * Copyright 2007 Network Engine for Objects in Lund AB [neotechnology.com]
 */
package org.neo4j.graphviz;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an object that can emit the properties of either a node or a
 * relationship. The invocation order for the methods of an Emitter should
 * always be: {@link #emitProperty(SourceType, String, Object)}*,
 * {@link #done()}.
 * @author Tobias Ivarsson
 */
public abstract class Emitter
{
	private enum PropertyType
	{
		// Scalar types
		STRING( "String", String.class )
		{
			@Override
			String format( Object object )
			{
				String string = ( String ) object;
				string = string.replace( "\\n", "\\\\n" );
				string = string.replace( "\\", "\\\\" );
				string = string.replace( "\"", "\\\"" );
				string = string.replace( "'", "\\\\'" );
				string = string.replace( "\n", "\\\\n" );
				return "'" + string + "'";
			}
		},
		INT( "int", Integer.class, int.class ), LONG( "long", Long.class,
		    long.class ), BOOLEAN( "boolean", Boolean.class, boolean.class ),
		SHORT( "short", Short.class, short.class ), CHAR( "char",
		    Character.class, char.class ),
		BYTE( "byte", Byte.class, byte.class ), FLOAT( "float", Float.class,
		    float.class ), DOUBLE( "double", Double.class, double.class ),
		// Array types
		STRING_ARRAY( "String[]", String[].class )
		{
			@Override
			String format( Object object )
			{
				return formatArray( ( String[] ) object, String.class );
			}
		},
		INT_ARRAY( "int[]", Integer[].class, int[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					int[] value = ( int[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Integer[] value = ( Integer[] ) object;
					return formatArray( value, Integer.class );
				}
			}
		},
		LONG_ARRAY( "long[]", Long[].class, long[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					long[] value = ( long[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Long[] value = ( Long[] ) object;
					return formatArray( value, Long.class );
				}
			}
		},
		BOOLEAN_ARRAY( "boolean[]", Boolean[].class, boolean[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					boolean[] value = ( boolean[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Boolean[] value = ( Boolean[] ) object;
					return formatArray( value, Boolean.class );
				}
			}
		},
		SHORT_ARRAY( "short[]", Short[].class, short[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					short[] value = ( short[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Short[] value = ( Short[] ) object;
					return formatArray( value, Short.class );
				}
			}
		},
		CHAR_ARRAY( "char[]", Character[].class, char[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					char[] value = ( char[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Character[] value = ( Character[] ) object;
					return formatArray( value, Character.class );
				}
			}
		},
		BYTE_ARRAY( "byte[]", Byte[].class, byte[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					byte[] value = ( byte[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Byte[] value = ( Byte[] ) object;
					return formatArray( value, Byte.class );
				}
			}
		},
		FLOAT_ARRAY( "float[]", Float[].class, float[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					float[] value = ( float[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Float[] value = ( Float[] ) object;
					return formatArray( value, Float.class );
				}
			}
		},
		DOUBLE_ARRAY( "double[]", Double[].class, double[].class )
		{
			@Override
			String format( Object object )
			{
				if ( object instanceof int[] )
				{
					double[] value = ( double[] ) object;
					return Arrays.toString( value );
				}
				else
				{
					Double[] value = ( Double[] ) object;
					return formatArray( value, Double.class );
				}
			}
		};
		private static final Map<Class<?>, PropertyType> typeMap = Collections
		    .unmodifiableMap( new HashMap<Class<?>, PropertyType>()
		    {
			    {
				    for ( PropertyType type : PropertyType.values() )
				    {
					    for ( Class<?> cls : type.types )
					    {
						    put( cls, type );
					    }
				    }
			    }
		    } );
		private final String typeName;
		private Class<?>[] types;

		private PropertyType( String typeName, Class<?>... types )
		{
			this.typeName = typeName;
			this.types = types;
		}

		String format( Object object )
		{
			return object.toString();
		}

		<T> String formatArray( T[] items, Class<T> type )
		{
			PropertyType formatter = typeMap.get( type );
			StringBuilder result = new StringBuilder( "[" );
			boolean addComma = false;
			for ( T item : items )
			{
				if ( addComma )
				{
					result.append( ", " );
				}
				result.append( formatter.format( item ) );
				addComma = true;
			}
			result.append( "]" );
			return result.toString();
		}
	}

	private final EmissionPolicy policy;

	/**
	 * Create a new Emitter.
	 * @param policy
	 *            The policy that determines what aspects of the graph to emit.
	 */
	public Emitter( EmissionPolicy policy )
	{
		this.policy = ( policy != null ) ? policy : EmissionPolicy.ACCEPT_ALL;
	}

	/**
	 * Returns a string representation of a property value.
	 * @param value
	 *            The value to get a representation of.
	 * @return A printable string that represents the given value.
	 */
	protected String escape( Object value )
	{
		PropertyType type = PropertyType.typeMap.get( value.getClass() );
		if ( type != null )
		{
			return type.format( value );
		}
		else
		{
			return value.toString();
		}
	}

	/**
	 * Returns a string representation of the type of a property value.
	 * @param value
	 *            The value to get the type of.
	 * @return A string representing the type of the given value.
	 */
	protected String typeOf( Object value )
	{
		PropertyType type = PropertyType.typeMap.get( value.getClass() );
		if ( type != null )
		{
			return type.typeName;
		}
		else
		{
			return value.getClass().toString();
		}
	}

	/**
	 * Emit a specified property. Default behavior is to invoke
	 * {@link #emitMapping(String, String, String)} with string representations
	 * of the property value and property type.
	 * @param from
	 * @param key
	 *            The property key.
	 * @param property
	 *            The property value.
	 */
	public void emitProperty( SourceType from, String key, Object property )
	{
		if ( policy.acceptProperty( from, key ) )
		{
			emitMapping( key, escape( property ), typeOf( property ) );
		}
	}

	/**
	 * Invoked when {@link #emitProperty(SourceType, String, Object)} has been
	 * invoked for all the properties of the node or relationship that this
	 * emitter was associated with.
	 */
	public abstract void done();

	/**
	 * Invoked by {@link #emitProperty(SourceType, String, Object)} with the key
	 * and string representations of the property value and property type.
	 * @param key
	 *            The property key.
	 * @param value
	 *            A string representation of the property value.
	 * @param type
	 *            A string representation of the property type.
	 */
	protected abstract void emitMapping( String key, String value, String type );
}
