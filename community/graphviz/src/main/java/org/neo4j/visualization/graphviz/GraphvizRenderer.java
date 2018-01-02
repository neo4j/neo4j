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

import java.io.IOException;
import java.io.PrintStream;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.visualization.GraphRenderer;
import org.neo4j.visualization.PropertyRenderer;

class GraphvizRenderer implements GraphRenderer<IOException>
{
	private final PrintStream stream;
	private final GraphStyle graphStyle;
	private final NodeStyle nodeStyle;
	private final RelationshipStyle edgeStyle;

	GraphvizRenderer( GraphStyle style, PrintStream stream ) throws IOException
	{
		this.stream = stream;
		nodeStyle = style.nodeStyle;
		edgeStyle = style.edgeStyle;
		graphStyle = style;
		graphStyle.emitGraphStart( stream );
	}

	public void done() throws IOException
	{
		graphStyle.emitGraphEnd( stream );
	}

	public PropertyRenderer<IOException> renderNode( Node node )
	    throws IOException
	{
		return new PropertyAdapter( node );
	}

	public PropertyRenderer<IOException> renderRelationship(
	    Relationship relationship ) throws IOException
	{
		return new PropertyAdapter( relationship );
	}

    public GraphvizRenderer renderSubgraph( String name ) throws IOException
    {
        return new GraphvizRenderer( graphStyle.getSubgraphStyle( name ), stream );
    }

	private class PropertyAdapter implements PropertyRenderer<IOException>
	{
		private final PropertyContainerStyle style;

		PropertyAdapter( Node node ) throws IOException
		{
			nodeStyle.emitNodeStart( stream, node );
			this.style = nodeStyle;
		}

		PropertyAdapter( Relationship relationship ) throws IOException
		{
			edgeStyle.emitRelationshipStart( stream, relationship );
			this.style = edgeStyle;
		}

		public void done() throws IOException
		{
			style.emitEnd( stream );
		}

		public void renderProperty( String propertyKey, Object propertyValue )
		    throws IOException
		{
			style.emitProperty( stream, propertyKey, propertyValue );
		}
	}
}
