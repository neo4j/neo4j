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
package org.neo4j.graphalgo.impl.path;

import org.neo4j.graphalgo.impl.util.LiteDepthFirstSelector;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import static org.neo4j.graphdb.traversal.Evaluators.atDepth;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;
import static org.neo4j.kernel.StandardExpander.toPathExpander;
import static org.neo4j.kernel.Traversal.bidirectionalTraversal;
import static org.neo4j.kernel.Traversal.traversal;

/**
 * Tries to find paths in a graph from a start node to an end node where the
 * length of found paths must be of a certain length. It also detects
 * "super nodes", i.e. nodes which have many relationships and only iterates
 * over such super nodes' relationships up to a supplied threshold. When that
 * threshold is reached such nodes are considered super nodes and are put on a
 * queue for later traversal. This makes it possible to find paths w/o having to
 * traverse heavy super nodes.
 *
 * @author Mattias Persson
 * @author Tobias Ivarsson
 */
public class ExactDepthPathFinder extends TraversalPathFinder
{
    private final PathExpander expander;
    private final int onDepth;
    private final int startThreshold;
    private final Uniqueness uniqueness;

    public ExactDepthPathFinder( RelationshipExpander expander, int onDepth, int startThreshold, boolean allowLoops )
    {
        this( toPathExpander( expander ), onDepth, startThreshold, allowLoops );
    }

    public ExactDepthPathFinder( PathExpander expander, int onDepth, int startThreshold, boolean allowLoops )
    {
        this.expander = expander;
        this.onDepth = onDepth;
        this.startThreshold = startThreshold;
        this.uniqueness = allowLoops ? Uniqueness.RELATIONSHIP_GLOBAL : Uniqueness.NODE_PATH;
    }

    @Override
    protected Traverser instantiateTraverser( Node start, Node end )
    {
        TraversalDescription side =
                traversal().breadthFirst().uniqueness( uniqueness ).order( new BranchOrderingPolicy()
                {
                    @Override
                    public BranchSelector create( TraversalBranch startSource, PathExpander expander )
                    {
                        return new LiteDepthFirstSelector( startSource, startThreshold, expander );
                    }
                } );
        return bidirectionalTraversal().startSide( side.expand( expander ).evaluator( toDepth( onDepth / 2 ) ) )
                .endSide( side.expand( expander.reverse() ).evaluator( toDepth( onDepth - onDepth / 2 ) ) )
                .collisionEvaluator( atDepth( onDepth ) )
                // TODO Level side selector will make the traversal return wrong result, why?
                //                .sideSelector( SideSelectorPolicies.LEVEL, onDepth )
                .traverse( start, end );
    }
}
