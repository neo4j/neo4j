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

import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;

import org.neo4j.function.Predicate;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.traversal.BidirectionalUniquenessFilter;
import org.neo4j.graphdb.traversal.BranchCollisionDetector;
import org.neo4j.graphdb.traversal.BranchSelector;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.SideSelector;
import org.neo4j.graphdb.traversal.SideSelectorPolicy;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalContext;
import org.neo4j.graphdb.traversal.UniquenessFilter;

class BidirectionalTraverserIterator extends AbstractTraverserIterator
{
    private final BranchCollisionDetector collisionDetector;
    private Iterator<Path> foundPaths;
    private SideSelector selector;
    private final Map<Direction, Side> sides = new EnumMap<>( Direction.class );
    private final BidirectionalUniquenessFilter uniqueness;

    private static class Side
    {
        private final MonoDirectionalTraversalDescription description;

        public Side( MonoDirectionalTraversalDescription description )
        {
            this.description = description;
        }
    }

    BidirectionalTraverserIterator( Resource resource,
                                    MonoDirectionalTraversalDescription start,
                                    MonoDirectionalTraversalDescription end,
                                    SideSelectorPolicy sideSelector,
                                    org.neo4j.graphdb.traversal.BranchCollisionPolicy collisionPolicy,
                                    PathEvaluator collisionEvaluator, int maxDepth,
                                    Iterable<Node> startNodes, Iterable<Node> endNodes )
    {
        super( resource );
        this.sides.put( Direction.OUTGOING, new Side( start ) );
        this.sides.put( Direction.INCOMING, new Side( end ) );
        this.uniqueness = makeSureStartAndEndHasSameUniqueness( start, end );

        // A little chicken-and-egg problem. This happens when constructing the start/end
        // selectors and they initially call evaluate() and isUniqueFirst, where the selector is used.
        // Solved this way for now, to have it return the start side to begin with.
        this.selector = fixedSide( Direction.OUTGOING );
        BranchSelector startSelector = start.branchOrdering.create(
                new AsOneStartBranch( this, startNodes, start.initialState, start.uniqueness ), start.expander );
        this.selector = fixedSide( Direction.INCOMING );
        BranchSelector endSelector = end.branchOrdering.create(
                new AsOneStartBranch( this, endNodes, end.initialState, start.uniqueness ), end.expander );

        this.selector = sideSelector.create( startSelector, endSelector, maxDepth );
        this.collisionDetector = collisionPolicy.create( collisionEvaluator,
                new Predicate<Path>()
                {
                    @Override
                    public boolean test( Path path )
                    {
                        return uniqueness.checkFull( path );
                    }
                } );
    }

    private BidirectionalUniquenessFilter makeSureStartAndEndHasSameUniqueness( MonoDirectionalTraversalDescription
            start,
                                                                   MonoDirectionalTraversalDescription end )
    {
        if ( !start.uniqueness.equals( end.uniqueness ) )
        {
            throw new IllegalArgumentException( "Start and end uniqueness factories differ, they need to be the " +
                    "same currently. Start side has " + start.uniqueness + ", end side has " + end.uniqueness );
        }

        boolean parameterDiffers = start.uniquenessParameter == null || end.uniquenessParameter == null ?
                start.uniquenessParameter != end.uniquenessParameter :
                !start.uniquenessParameter.equals( end.uniquenessParameter );
        if ( parameterDiffers )
        {
            throw new IllegalArgumentException( "Start and end uniqueness parameters differ, they need to be the " +
                    "same currently. Start side has " + start.uniquenessParameter + ", " +
                    "end side has " + end.uniquenessParameter );
        }

        UniquenessFilter uniqueness = start.uniqueness.create( start.uniquenessParameter );
        if ( !(uniqueness instanceof BidirectionalUniquenessFilter) )
        {
            throw new IllegalArgumentException( "You must supply a BidirectionalUniquenessFilter, " +
                    "not just a UniquenessFilter." );
        }
        return (BidirectionalUniquenessFilter) uniqueness;
    }

    private SideSelector fixedSide( final Direction direction )    {
        return new SideSelector()
        {
            @Override
            public TraversalBranch next( TraversalContext metadata )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Direction currentSide()
            {
                return direction;
            }
        };
    }

    @Override
    protected Path fetchNextOrNull()
    {
        if ( foundPaths != null )
        {
            if ( foundPaths.hasNext() )
            {
                numberOfPathsReturned++;
                Path next = foundPaths.next();
                return next;
            }
            foundPaths = null;
        }

        TraversalBranch result = null;
        while ( true )
        {
            result = selector.next( this );
            if ( result == null )
            {
                close();
                return null;
            }
            Iterable<Path> pathCollisions = collisionDetector.evaluate( result, selector.currentSide() );
            if ( pathCollisions != null )
            {
                foundPaths = pathCollisions.iterator();
                if ( foundPaths.hasNext() )
                {
                    numberOfPathsReturned++;
                    Path next = foundPaths.next();
                    return next;
                }
            }
        }
    }

    private Side currentSideDescription()
    {
        return sides.get( selector.currentSide() );
    }

    @Override
    public Evaluation evaluate( TraversalBranch branch, BranchState state )
    {
        return currentSideDescription().description.evaluator.evaluate( branch, state );
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
