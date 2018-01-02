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

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

/**
 * @param <E>
 *            A base exception type that can be thrown by the methods of this
 *            renderer.
 */
public interface GraphRenderer<E extends Throwable>
{
	/**
	 * Renders a node.
	 * @param node
	 *            the node to render.
	 * @return an object capable of rendering the properties of the node, or
	 *         <code>null</code> to not render properties externally.
	 * @throws E
	 *             if an error occurs when rendering the node.
	 */
	PropertyRenderer<E> renderNode( Node node ) throws E;

	/**
	 * Renders a relationship.
	 * @param relationship
	 *            the relationship to render.
	 * @return an object capable of rendering the properties of the
	 *         relationship, or <code>null</code> to not render properties
	 *         externally.
	 * @throws E
	 *             if an error occurs when rendering the relationship.
	 */
	PropertyRenderer<E> renderRelationship( Relationship relationship )
	    throws E;

	/**
	 * Invoked when the entire graph has been rendered.
	 * @throws E
	 *             if an error occurs.
	 */
	void done() throws E;

    GraphRenderer<E> renderSubgraph( String name ) throws E;
}
