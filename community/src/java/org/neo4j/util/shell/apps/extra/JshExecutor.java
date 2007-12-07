package org.neo4j.util.shell.apps.extra;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.Map;

import org.neo4j.util.shell.Output;
import org.neo4j.util.shell.Session;
import org.neo4j.util.shell.ShellException;

public class JshExecutor extends ScriptExecutor
{
	static
	{
		String jythonSystemVariableName = "python.home";
		if ( System.getProperty( jythonSystemVariableName ) == null )
		{
			String variable = tryFindEnvironmentVariable( "JYTHON_HOME",
				"JYTHONHOME", "JYTHON", "PYTHON_HOME", "PYTHONHOME", "PYTHON" );
			variable = variable == null ? "/usr/local/jython" : variable;
			System.setProperty( jythonSystemVariableName, variable );
		}
	}
	
	private static String tryFindEnvironmentVariable( String... examples )
	{
		Map<String, String> env = System.getenv();
		for ( String example : examples )
		{
			for ( String envKey : env.keySet() )
			{
				if ( envKey.contains( example ) )
				{
					return env.get( envKey );
				}
			}
		}
		return null;
	}
	
	/**
	 * The {@link Session} key used to read which paths (folders on disk) to
	 * list groovy scripts from.
	 */
	public static final String PATH_STRING = "JSH_PATH";
	
	/**
	 * Default paths to use if no paths are specified by the
	 * {@link #PATH_STRING}.
	 */
	public static final String DEFAULT_PATHS =
		".:script:src" + File.separator + "script";
	
	/**
	 * The class name which represents the PythonInterpreter class.
	 */
	public static final String INTERPRETER_CLASS =
		"org.python.util.PythonInterpreter";

	@Override
	protected void ensureDependenciesAreInClasspath() throws ShellException
	{
		try
		{
			Class.forName( INTERPRETER_CLASS );
		}
		catch ( ClassNotFoundException e )
		{
			throw new ShellException( "Jython not found in the classpath", e );
		}
	}
	
	@Override
	protected String getDefaultPaths()
	{
		return ".:script:src" + File.separator + "script";
	}
	
	@Override
	protected String getPathKey()
	{
		return PATH_STRING;
	}
	
	@Override
	protected Object newInterpreter( String[] paths ) throws ShellException
	{
		try
		{
			return Class.forName( INTERPRETER_CLASS ).newInstance();
		}
		catch ( Exception e )
		{
			throw new ShellException( "Invalid jython classes", e );
		}
	}
	
	@Override
	protected void runScript( Object interpreter, String scriptName,
	    Map<String, Object> properties, String[] paths ) throws ShellException
	{
		File scriptFile = findScriptFile( scriptName, paths );
		try
		{
			Output out = ( Output ) properties.remove( "out" );
			interpreter.getClass().getMethod( "setOut", Writer.class ).invoke(
				interpreter, new OutputWriter( out ) );
			Method setMethod = interpreter.getClass().getMethod( "set",
				String.class, Object.class );
			for ( String key : properties.keySet() )
			{
				setMethod.invoke( interpreter, key, properties.get( key ) );
			}
			interpreter.getClass().getMethod( "execfile", String.class )
				.invoke( interpreter, scriptFile.getAbsolutePath() );
		}
		catch ( Exception e )
		{
			// Don't pass the exception on because the client most certainly
			// doesn't have groovy in the classpath.
			throw new ShellException( "Jython exception: " +
				this.stackTraceAsString( e ) );
		}
	}
	
	private File findScriptFile( String scriptName, String[] paths )
		throws ShellException
	{
		for ( String path : paths )
		{
			File result = findScriptFile( scriptName, path );
			if ( result != null )
			{
				return result;
			}
		}
		throw new ShellException( "No script '" + scriptName + "' found" );
	}
	
	private File findScriptFile( String scriptName, String path )
	{
		File pathFile = new File( path );
		if ( !pathFile.exists() )
		{
			return null;
		}
		
		for ( File file : pathFile.listFiles() )
		{
			String name = file.getName();
			int dotIndex = name.lastIndexOf( '.' );
			name = dotIndex == -1 ? name : name.substring( 0, dotIndex );
			String extension = dotIndex == -1 ? null :
				file.getName().substring( dotIndex + 1 );
			if ( scriptName.equals( name ) && ( extension == null ||
				extension.toLowerCase().equals( "py" ) ) )
			{
				return file;
			}
		}
		return null;
	}
	
	private static class OutputWriter extends Writer
	{
		private Output out;
		
		OutputWriter( Output out )
		{
			this.out = out;
		}
		
		@Override
		public Writer append( char c ) throws IOException
		{
			out.append( c );
			return this;
		}
		
		@Override
		public Writer append( CharSequence csq, int start, int end )
		    throws IOException
		{
			out.append( csq, start, end );
			return this;
		}
		
		@Override
		public Writer append( CharSequence csq ) throws IOException
		{
			out.append( csq );
			return this;
		}
		
		@Override
		public void close() throws IOException
		{
		}
		
		@Override
		public void flush() throws IOException
		{
		}
		
		@Override
		public void write( char[] cbuf, int off, int len ) throws IOException
		{
			out.print( new String( cbuf, off, len ) );
		}
		
		@Override
		public void write( char[] cbuf ) throws IOException
		{
			out.print( new String( cbuf ) );
		}
		
		@Override
		public void write( int c ) throws IOException
		{
			out.print( String.valueOf( c ) );
		}
		
		@Override
		public void write( String str, int off, int len ) throws IOException
		{
			char[] cbuf = new char[ len ];
			str.getChars( off, off + len, cbuf, 0 );
			write( cbuf, off, len );
		}
		
		@Override
		public void write( String str ) throws IOException
		{
			out.print( str );
		}
	}
}
