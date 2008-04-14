/*
 * Copyright 2007 Network Engine for Objects in Lund AB [neotechnology.com]
 */
package org.neo4j.graphviz;

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
	@SuppressWarnings( { "unchecked", "serial" } )
	private static final Map<Class, String> types = Collections
	    .unmodifiableMap( new HashMap<Class, String>()
	    {
		    {
			    put( String.class, "String" );
			    put( Integer.class, "int" );
			    put( Long.class, "long" );
			    put( Boolean.class, "boolean" );
			    put( Short.class, "short" );
			    put( Character.class, "char" );
			    put( Byte.class, "byte" );
			    put( Float.class, "float" );
			    put( Double.class, "double" );
		    }
	    } );
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
		if ( value instanceof String )
		{
			String string = ( String ) value;
			string = string.replace( "\\n", "\\\\n" );
			string = string.replace( "\\", "\\\\" );
			string = string.replace( "\"", "\\\"" );
			string = string.replace( "'", "\\\\'" );
			string = string.replace( "\n", "\\\\n" );
			return "'" + string + "'";
		}
		else if ( value instanceof String[] )
		{
			String[] array = ( String[] ) value;
			StringBuilder result = new StringBuilder( "[" );
			for ( String string : array )
			{
				result.append( escape( string ) );
			}
			result.append( "]" );
			return result.toString();
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
		String result = null;
		if ( value != null )
		{
			result = types.get( value.getClass() );
		}
		if ( result != null )
		{
			return result;
		}
		else
		{
			return "Object";
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
