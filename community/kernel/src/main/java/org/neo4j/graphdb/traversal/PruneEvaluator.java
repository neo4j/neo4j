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

import org.neo4j.graphdb.Path;

/**
 * An evaluator which can "cut off" relationships so that they will not be
 * traversed in the ongoing traversal. For any given position a prune evaluator
 * can decide whether or not to prune whatever is beyond (i.e. after) that
 * position or not.
 *
 * @deprecated because of the introduction of {@link Evaluator} which combines
 * {@link PruneEvaluator} and filtering ({@link org.neo4j.function.Predicate} of {@link Path}s).
 */
public interface PruneEvaluator
{
    /**
     * Default {@link PruneEvaluator}, does not prune any parts of the
     * traversal.
     */
    PruneEvaluator NONE = new PruneEvaluator()
    {
        public boolean pruneAfter( Path position )
        {
            return false;
        }
    };

    /**
     * Decides whether or not to prune after {@code position}. If {@code true}
     * is returned the position won't be expanded and traversals won't be made
     * beyond that position.
     *
     * @param position the {@link Path position} to decide whether or not to
     *            prune after.
     * @return whether or not to prune after {@code position}.
     */
    boolean pruneAfter( Path position );
}
