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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.neo4j.shell.Output;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;

import static org.neo4j.shell.TextUtil.tokenizeStringWithQuotes;

/**
 * Executes groovy scripts purely via reflection
 */
public abstract class ScriptExecutor
{
	protected abstract String getPathKey();

	protected String getDefaultPaths()
	{
		return ".:src:src" + File.separator + "script";
	}

	/**
	 * Executes a groovy script (with arguments) defined in {@code line}.
	 * @param line the line which defines the groovy script with arguments.
	 * @param session the {@link Session} to include as argument in groovy.
	 * @param out the {@link Output} to include as argument in groovy.
	 * @throws ShellException if the execution of a groovy script fails.
	 */
	public void execute( String line, Session session, Output out )
		throws Exception
	{
		this.ensureDependenciesAreInClasspath();
		if ( line == null || line.trim().length() == 0 )
		{
			return;
		}

		List<String> pathList = this.getEnvPaths( session );
		String[] paths = pathList.toArray( new String[ pathList.size() ] );
		Object interpreter = this.newInterpreter( paths );
		Map<String, Object> properties = new HashMap<>();
		properties.put( "out", out );
		properties.put( "session", session );
		this.runScripts( interpreter, properties, line, paths );
	}

	private void runScripts( Object interpreter,
		Map<String, Object> properties, String line, String[] paths )
		throws Exception
	{
		ArgReader reader = new ArgReader( tokenizeStringWithQuotes( line ) );
		while ( reader.hasNext() )
		{
			String arg = reader.next();
			if ( arg.startsWith( "--" ) )
			{
				String[] scriptArgs = getScriptArgs( reader );
				String scriptName = arg.substring( 2 );
				Map<String, Object> props = new HashMap<>( properties );
				props.put( "args", scriptArgs );
				this.runScript( interpreter, scriptName, props, paths );
			}
		}
	}

	protected abstract void runScript( Object interpreter,
		String scriptName, Map<String, Object> properties, String[] paths )
		throws Exception;

	private String[] getScriptArgs( ArgReader reader )
	{
		reader.mark();
		try
		{
			ArrayList<String> list = new ArrayList<>();
			while ( reader.hasNext() )
			{
				String arg = reader.next();
				if ( arg.startsWith( "--" ) )
				{
					break;
				}
				list.add( arg );
				reader.mark();
			}
			return list.toArray( new String[ list.size() ] );
		}
		finally
		{
			reader.flip();
		}
	}

	private List<String> getEnvPaths( Session session )
		throws ShellException
	{
	    List<String> list = new ArrayList<>();
	    collectPaths( list, ( String ) session.get( getPathKey() ) );
	    collectPaths( list, getDefaultPaths() );
	    return list;
	}

	private void collectPaths( List<String> paths, String pathString )
	{
		if ( pathString != null && pathString.trim().length() > 0 )
		{
			for ( String path : pathString.split( ":" ) )
			{
				paths.add( path );
			}
		}
	}

	protected abstract Object newInterpreter( String[] paths )
		throws Exception;

	protected abstract void ensureDependenciesAreInClasspath()
		throws Exception;

	static class ArgReader implements Iterator<String>
	{
		private static final int START_INDEX = -1;

		private int index = START_INDEX;
		private final String[] args;
		private Integer mark;

		ArgReader( String[] args )
		{
			this.args = args;
		}

		@Override
        public boolean hasNext()
		{
			return this.index + 1 < this.args.length;
		}

		@Override
        public String next()
		{
			if ( !hasNext() )
			{
				throw new NoSuchElementException();
			}
			this.index++;
			return this.args[ this.index ];
		}

		@Override
        public void remove()
		{
			throw new UnsupportedOperationException();
		}

		/**
		 * Marks the position so that a call to {@link #flip()} returns to that
		 * position.
		 */
		public void mark()
		{
			this.mark = this.index;
		}

		/**
		 * Flips back to the position defined in {@link #mark()}.
		 */
		public void flip()
		{
			if ( this.mark == null )
			{
				throw new IllegalStateException();
			}
			this.index = this.mark;
			this.mark = null;
		}
	}
}
