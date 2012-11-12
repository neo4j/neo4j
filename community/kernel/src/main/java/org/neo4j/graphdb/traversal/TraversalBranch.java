/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import org.neo4j.graphdb.RelationshipExpander;

/**
 * Represents a {@link Path position} and a {@link RelationshipExpander} with a
 * traversal context, for example parent and an iterator of relationships to go
 * next. It's a base to write a {@link BranchSelector} on top of.
 */
public interface TraversalBranch
{
    /**
     * The parent expansion source which created this {@link TraversalBranch}.
     * @return the parent of this expansion source.
     */
    TraversalBranch parent();

    /**
     * The position represented by this expansion source.
     * @return the position represented by this expansion source.
     */
    Path position();

    /**
     * The depth for this expansion source compared to the start node of the
     * traversal.
     * @return the depth of this expansion source.
     */
    int depth();

    /**
     * The node for this expansion source.
     * @return the node for this expansion source.
     */
    Node node();

    /**
     * The relationship for this expansion source. It's the relationship
     * which was traversed to get to this expansion source.
     * @return the relationship for this expansion source.
     */
    Relationship relationship();

    /**
     * Returns the next expansion source from the expanded relationships
     * from the current node.
     *
     * @return the next expansion source from this expansion source.
     */
    TraversalBranch next();

    /**
     * Returns the number of relationships this expansion source has expanded.
     * In this count isn't included the relationship which led to coming here
     * (since that could also be traversed, although skipped, when expanding
     * this source).
     *
     * @return the number of relationships this expansion source has expanded.
     */
    int expanded();
    
    Evaluation evaluation();
    
    void initialize();
}
