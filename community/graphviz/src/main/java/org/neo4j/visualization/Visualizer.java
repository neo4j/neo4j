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
package org.neo4j.visualization;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.walk.Visitor;

/**
 * Traverses a graph and outputs the result to an external receiver.
 * @param <E>
 *            A base exception type that can be thrown by the methods of this
 *            visualizer.
 */
public class Visualizer<E extends Throwable> implements Visitor<Void, E>
{
	private final GraphRenderer<E> renderer;
	private Set<Relationship> visitedRelationships = new HashSet<Relationship>();
	private Set<Node> visitedNodes = new HashSet<Node>();

	/**
	 * Creates a new visualizer.
	 * @param renderer
	 *            An object capable of rendering the different parts of a graph.
	 */
	public Visualizer( GraphRenderer<E> renderer )
	{
		this.renderer = renderer;
	}

	public Void done() throws E
	{
		renderer.done();
		return null;
	}

	public void visitNode( Node node ) throws E
	{
		if ( visitedNodes.add( node ) )
		{
			renderProperties( renderer.renderNode( node ), node );
		}
	}

	public void visitRelationship( Relationship relationship ) throws E
	{
		if ( visitedRelationships.add( relationship ) )
		{
			renderProperties( renderer.renderRelationship( relationship ),
			    relationship );
		}
	}

    public Visitor<Void, E> visitSubgraph( String name ) throws E
    {
        return new Visualizer<E>( renderer.renderSubgraph( name ) );
    }

	private void renderProperties( PropertyRenderer<E> propertyRenderer,
	    PropertyContainer container ) throws E
	{
		for ( String key : container.getPropertyKeys() )
		{
			propertyRenderer.renderProperty( key, container.getProperty( key ) );
		}
		propertyRenderer.done();
	}
}
