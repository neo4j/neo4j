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
package org.neo4j.visualization.graphviz;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.visualization.Visualizer;
import org.neo4j.walk.Walker;

/**
 * An object that writes a graph to a specified destination in graphviz dot
 * format.
 */
public final class GraphvizWriter
{
	private final GraphStyle style;

	/**
	 * Create a new Graphviz writer.
	 * @param configuration
	 *            the style parameters determining how the style of the output
	 *            of this writer.
	 */
	public GraphvizWriter( StyleParameter... configuration )
	{
		this( new GraphStyle( configuration ) );
	}

	public GraphvizWriter( GraphStyle style )
	{
		this.style = style;
	}

	/**
	 * Emit a graph to a file in graphviz format using this writer.
	 * @param dest
	 *            the file to write the graph to.
	 * @param walker
	 *            a walker that walks the graph to emit.
	 * @throws IOException
	 *             if there is an error in outputting to the specified file.
	 */
	public void emit( File dest, Walker walker ) throws IOException
	{
		OutputStream stream = new FileOutputStream( dest );
		emit( stream, walker );
		stream.close();
	}

	/**
	 * Emit a graph to an output stream in graphviz format using this writer.
	 * @param outputStream
	 *            the stream to write the graph to.
	 * @param walker
	 *            a walker that walks the graph to emit.
	 * @throws IOException
	 *             if there is an error in outputting to the specified stream.
	 */
	public void emit( OutputStream outputStream, Walker walker )
	    throws IOException
	{
		if ( outputStream instanceof PrintStream )
		{
			emit( walker, new GraphvizRenderer( style,
			    ( PrintStream ) outputStream ) );
		}
		else
		{
			emit( walker, new GraphvizRenderer( style, new PrintStream( outputStream, true, "UTF-8" ) ) );
		}
	}

	private void emit( Walker walker, GraphvizRenderer renderer )
	    throws IOException
	{
		walker.accept( new Visualizer<IOException>( renderer ) );
	}
}
