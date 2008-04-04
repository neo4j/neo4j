package org.neo4j.graphviz;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class Emitter
{
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

	public void emitProperty( String key, Object property )
	{
		emitMapping( key, escape( property ), typeOf( property ) );
	}

	public abstract void done();

	protected abstract void emitMapping( String key, String value, String type );
}
