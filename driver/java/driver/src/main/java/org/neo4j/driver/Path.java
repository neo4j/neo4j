/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.driver;

/**
 * A <strong>Path</strong> is a directed sequence of relationships between two nodes. This generally
 * represents a <em>traversal</em> or <em>walk</em> through a graph and maintains a direction separate
 * from that of any relationships traversed.
 * <p>
 * It is allowed to be of size 0, meaning there are no relationships in it. In this case,
 * it contains only a single node which is both the start and the end of the path.
 * <p>
 * <pre>
 *     Path routeToStockholm = ..;
 *
 *     // Work with each segment of the path
 *     for( Segment segment : routeToStockholm )
 *     {
 *
 *     }
 * </pre>
 */
public interface Path extends Iterable<Path.Segment>
{
    /**
     * A segment combines a relationship in a path with a start and end node that describe the traversal direction
     * for that relationship. This exists because the relationship has a direction between the two nodes that is
     * separate and potentially different from the direction of the path.
     * {@code
     * Path: (n1)-[r1]->(n2)<-[r2]-(n3)
     * Segment 1: (n1)-[r1]->(n2)
     * Segment 2: (n2)<-[r2]-(n3)
     * }
     */
    interface Segment extends Directed<Node>
    {
        /** @return the relationship underlying this path segment */
        Relationship relationship();
    }

    /** @return the start node of this path */
    Node start();

    /** @return the end node of this path */
    Node end();

    /** @return the number of segments in this path, which will be the same as the number of relationships */
    long length();

    /**
     * @param node the node to check for
     * @return true if the specified node is contained in this path
     */
    boolean contains( Node node );

    /**
     * @param relationship the relationship to check for
     * @return true if the specified relationship is contained in this path
     */
    boolean contains( Relationship relationship );

    /**
     * Create an iterable over the nodes in this path, nodes will appear in the same order as they appear
     * in the path.
     *
     * @return an {@link java.lang.Iterable} of all nodes in this path
     */
    Iterable<Node> nodes();

    /**
     * Create an iterable over the relationships in this path. The relationships will appear in the same order as they
     * appear in the path.
     *
     * @return an {@link java.lang.Iterable} of all relationships in this path
     */
    Iterable<Relationship> relationships();
}
