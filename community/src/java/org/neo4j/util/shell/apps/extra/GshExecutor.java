package org.neo4j.util.shell.apps.extra;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.rmi.RemoteException;
import java.util.Map;

import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.ShellException;

/**
 * Executes groovy scripts purely via reflection
 */
public class GshExecutor extends ScriptExecutor
{
	private static final String BINDING_CLASS = "groovy.lang.Binding";
	
	private static final String ENGINE_CLASS = "groovy.util.GroovyScriptEngine";
	
	@Override
	protected String getPathKey()
	{
		return "GSH_PATH";
	}

	@Override
	protected void runScript( Object groovyScriptEngine,
		String scriptName, Map<String, Object> properties, String[] paths )
		throws ShellException
	{
		try
		{
			properties.put( "out",
				new GshOutput( ( Output ) properties.get( "out" ) ) );
			Object binding = this.newGroovyBinding( properties );
			Method runMethod = groovyScriptEngine.getClass().getMethod(
				"run", String.class, binding.getClass() );
			runMethod.invoke( groovyScriptEngine, scriptName + ".groovy",
				binding );
		}
		catch ( Exception e )
		{
			// Don't pass the exception on because the client most certainly
			// doesn't have groovy in the classpath.
			throw new ShellException( "Groovy exception: " +
				this.findProperMessage( e ) + "\n" +
				"\n----------[ACTUAL EXCEPTION]----------" + "\n" +
				this.stackTraceAsString( e ) +
				"----------[END OF ACTUAL EXCEPTION]----------\n" );
		}
	}
	
	private Object newGroovyBinding( Map<String, Object> properties )
		throws ShellException
	{
		try
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
		catch ( Exception e )
		{
			throw new ShellException( "Invalid groovy classes", e );
		}
	}

	@Override
	protected Object newInterpreter( String[] paths )
		throws ShellException
	{
		try
		{
			Class<?> cls = Class.forName( ENGINE_CLASS );
			return cls.getConstructor( String[].class ).newInstance(
				new Object[] { paths } );
		}
		catch ( Exception e )
		{
			throw new ShellException( "Invalid groovy classes", e );
		}
	}
	
	@Override
	protected void ensureDependenciesAreInClasspath() throws ShellException
	{
		try
		{
			Class.forName( BINDING_CLASS );
		}
		catch ( ClassNotFoundException e )
		{
			throw new ShellException( "Groovy not found in the classpath", e );
		}
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
