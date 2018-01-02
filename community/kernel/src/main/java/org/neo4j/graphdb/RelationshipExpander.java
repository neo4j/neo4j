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
package org.neo4j.graphdb;

/**
 * An expander of relationships. It's a flexible way of getting relationships
 * from a node.
 * 
 * @deprecated in favor or {@link PathExpander}. A {@link RelationshipExpander}
 * can still be wrapped via {@link org.neo4j.kernel.StandardExpander#toPathExpander(RelationshipExpander)}
 * to act like a {@link PathExpander}.
 */
public interface RelationshipExpander
{
    /**
     * Returns relationships for a {@link Node}.
     *
     * @param node the node to expand.
     * @return the relationships to return for the {@code node}.
     */
    Iterable<Relationship> expand( Node node );

    /**
     * Returns a new instance with the exact expansion logic, but reversed.
     * For example if this expander is set to expand:
     * <ul>
     *   <li>KNOWS : OUTGOING</li>
     *   <li>LIVES : INCOMING</li>
     * </ul>
     * Then the reversed expander will be set to expand:
     * <ul>
     *   <li>KNOWS : INCOMING</li>
     *   <li>LIVES : OUTGOING</li>
     * </ul>
     * 
     * @return a reversed {@link RelationshipExpander}.
     */
    RelationshipExpander reversed();
}
