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
import java.io.Writer;
import java.lang.reflect.Method;
import java.util.Map;

import org.neo4j.shell.Output;
import org.neo4j.shell.ShellException;

/**
 * Runs Python scripts.
 */
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
	 * The class name which represents the PythonInterpreter class.
	 */
	private static final String INTERPRETER_CLASS =
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
			throw new ShellException( "Jython not found in the classpath" );
		}
	}
	
	@Override
	protected String getPathKey()
	{
		return "JSH_PATH";
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
			throw new ShellException( "Invalid jython classes" );
		}
	}
	
	@Override
	protected void runScript( Object interpreter, String scriptName,
	    Map<String, Object> properties, String[] paths ) throws Exception
	{
		File scriptFile = findScriptFile( scriptName, paths );
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
