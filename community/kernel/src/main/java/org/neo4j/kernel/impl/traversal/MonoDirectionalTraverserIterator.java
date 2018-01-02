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
package org.neo4j.kernel.impl.traversal;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.graphdb.traversal.UniquenessFilter;

class MonoDirectionalTraverserIterator extends AbstractTraverserIterator
{
    private final BranchSelector selector;
    private final PathEvaluator evaluator;
    private final UniquenessFilter uniqueness;

    MonoDirectionalTraverserIterator( Resource resource, UniquenessFilter uniqueness, PathExpander expander,
                                      BranchOrderingPolicy order, PathEvaluator evaluator, Iterable<Node> startNodes,
                                      InitialBranchState initialState, UniquenessFactory uniquenessFactory )
    {
        super( resource );
        this.uniqueness = uniqueness;
        this.evaluator = evaluator;
        this.selector = order.create( new AsOneStartBranch( this, startNodes, initialState, uniquenessFactory ),
                expander );
    }

    @Override
    public Evaluation evaluate( TraversalBranch branch, BranchState state )
    {
        return evaluator.evaluate( branch, state );
    }

    @Override
    protected Path fetchNextOrNull()
    {
        TraversalBranch result;
        while ( true )
        {
            result = selector.next( this );
            if ( result == null )
            {
                close();
                return null;
            }
            if ( result.includes() )
            {
                numberOfPathsReturned++;
                return result;
            }
        }
    }

    @Override
    public boolean isUniqueFirst( TraversalBranch branch )
    {
        return uniqueness.checkFirst( branch );
    }

    @Override
    public boolean isUnique( TraversalBranch branch )
    {
        return uniqueness.check( branch );
    }
}