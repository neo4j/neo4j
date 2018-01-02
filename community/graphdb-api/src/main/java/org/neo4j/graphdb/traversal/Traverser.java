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
package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

/**
 * This interface represents the traverser which is used to step through the
 * results of a traversal. Each step can be represented in different ways. The
 * default is as {@link Path} objects which all other representations can be
 * derived from, i.e {@link Node} or {@link Relationship}. Each step
 * can also be represented in one of those representations directly.
 */
public interface Traverser extends ResourceIterable<Path>
{
    /**
     * Represents the traversal in the form of {@link Node}s. This is a
     * convenient way to iterate over {@link Path}s and get the
     * {@link Path#endNode()} for each position.
     *
     * @return the traversal in the form of {@link Node} objects.
     */
    ResourceIterable<Node> nodes();

    /**
     * Represents the traversal in the form of {@link Relationship}s. This is a
     * convenient way to iterate over {@link Path}s and get the
     * {@link Path#lastRelationship()} for each position.
     *
     * @return the traversal in the form of {@link Relationship} objects.
     */
    ResourceIterable<Relationship> relationships();

    /**
     * Represents the traversal in the form of {@link Path}s.
     * When a traversal is done and haven't been fully iterated through,
     * it should be {@link ResourceIterator#close() closed}.
     *
     * @return the traversal in the form of {@link Path} objects.
     */
    @Override
    ResourceIterator<Path> iterator();

    /**
     * @return the {@link TraversalMetadata} from the last traversal performed,
     * or being performed by this traverser.
     */
    TraversalMetadata metadata();
}
