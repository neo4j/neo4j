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
package org.neo4j.shell.apps.extra;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.shell.Output;

/**
 * Executes groovy scripts purely via reflection
 */
public class GshExecutor extends ScriptExecutor
{
	private static final String BINDING_CLASS = "groovy.lang.Binding";
	
	private static final String ENGINE_CLASS = "groovy.util.GroovyScriptEngine";
	
	@Override
    protected String getDefaultPaths()
    {
	    return super.getDefaultPaths() + ":" +
	            "src" + File.separator + "main" + File.separator + "groovy";
    }

    @Override
	protected String getPathKey()
	{
		return "GSH_PATH";
	}

	@Override
	protected void runScript( Object groovyScriptEngine,
		String scriptName, Map<String, Object> properties, String[] paths )
		throws Exception
	{
		properties.put( "out",
			new GshOutput( ( Output ) properties.get( "out" ) ) );
		Object binding = this.newGroovyBinding( properties );
		Method runMethod = groovyScriptEngine.getClass().getMethod(
			"run", String.class, binding.getClass() );
		runMethod.invoke( groovyScriptEngine, scriptName + ".groovy",
			binding );
	}
	
	private Object newGroovyBinding( Map<String, Object> properties )
		throws Exception
	{
		Class<?> cls = Class.forName( BINDING_CLASS );
		Object binding = cls.newInstance();
		Method setPropertyMethod =
			cls.getMethod( "setProperty", String.class, Object.class );
		for ( String key : properties.keySet() )
		{
			setPropertyMethod.invoke( binding, key, properties.get( key ) );
		}
		return binding;
	}

	@Override
	protected Object newInterpreter( String[] paths )
		throws Exception
	{
		Class<?> cls = Class.forName( ENGINE_CLASS );
		return cls.getConstructor( String[].class ).newInstance(
			new Object[] { paths } );
	}
	
	@Override
	protected void ensureDependenciesAreInClasspath() throws Exception
	{
	    Class.forName( BINDING_CLASS );
	}

	/**
	 * A wrapper for a supplied {@link Output} to correct a bug where a call
	 * to "println" or "print" with a GString or another object would use
	 * System.out instead of the right output instance.
	 */
	public static class GshOutput implements Output
	{
		private Output source;
		
		GshOutput( Output output )
		{
			this.source = output;
		}

		public void print( Serializable object ) throws RemoteException
		{
			source.print( object );
		}
		
		public void println( Serializable object ) throws RemoteException
		{
			source.println( object );
		}
		
		public Appendable append( char c ) throws IOException
		{
			return source.append( c );
		}
		
		public Appendable append( CharSequence csq, int start, int end )
		    throws IOException
		{
			return source.append( csq, start, end );
		}
		
		public Appendable append( CharSequence csq ) throws IOException
		{
			return source.append( csq );
		}
		
		/**
		 * Prints an object to the wrapped {@link Output}.
		 * @param object the object to print.
		 * @throws RemoteException RMI error.
		 */
		public void print( Object object ) throws RemoteException
		{
			source.print( object.toString() );
		}
		
		public void println() throws RemoteException
		{
			source.println();
		}
		
		/**
		 * Prints an object to the wrapped {@link Output}.
		 * @param object the object to print.
		 * @throws RemoteException RMI error.
		 */
		public void println( Object object ) throws RemoteException
		{
			source.println( object.toString() );
		}
	}
}
