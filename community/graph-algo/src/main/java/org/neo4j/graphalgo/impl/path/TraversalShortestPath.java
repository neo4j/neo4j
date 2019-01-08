/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphalgo.impl.path;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

import static org.neo4j.graphdb.traversal.Evaluators.toDepth;
import static org.neo4j.graphdb.traversal.SideSelectorPolicies.LEVEL_STOP_DESCENT_ON_RESULT;
import static org.neo4j.graphdb.traversal.Uniqueness.NODE_PATH;

/**
 * Implements shortest path algorithm, see {@link ShortestPath}, but using
 * the traversal framework straight off with the bidirectional traversal feature.
 *
 * It's still experimental and slightly slower than the highly optimized
 * {@link ShortestPath} implementation.
 */
public class TraversalShortestPath extends TraversalPathFinder
{
    private final PathExpander expander;
    private final int maxDepth;
    private final Integer maxResultCount;

    public TraversalShortestPath( PathExpander expander, int maxDepth )
    {
        this.expander = expander;
        this.maxDepth = maxDepth;
        this.maxResultCount = null;
    }

    public TraversalShortestPath( PathExpander expander, int maxDepth, int maxResultCount )
    {
        this.expander = expander;
        this.maxDepth = maxDepth;
        this.maxResultCount = maxResultCount;
    }

    @Override
    protected Traverser instantiateTraverser( Node start, Node end )
    {
        GraphDatabaseService db = start.getGraphDatabase();
        TraversalDescription sideBase = db.traversalDescription().breadthFirst().uniqueness( NODE_PATH );
        return db.bidirectionalTraversalDescription().mirroredSides( sideBase.expand( expander ) )
            .sideSelector( LEVEL_STOP_DESCENT_ON_RESULT, maxDepth )
            .collisionEvaluator( toDepth( maxDepth ) )
            .traverse( start, end );
    }

    @Override
    protected Integer maxResultCount()
    {
        return maxResultCount;
    }
}
