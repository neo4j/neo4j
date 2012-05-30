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
package org.neo4j.kernel.impl.traversal;

import static org.neo4j.kernel.Traversal.traversal;
import static org.neo4j.kernel.impl.traversal.TraversalDescriptionImpl.addEvaluator;
import static org.neo4j.kernel.impl.traversal.TraversalDescriptionImpl.nullCheck;

import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.SideSelectorPolicy;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.SideSelectorPolicies;

public class BidirectionalTraversalDescriptionImpl implements BidirectionalTraversalDescription
{
    final TraversalDescription start;
    final TraversalDescription end;
    final Evaluator collisionEvaluator;
    final SideSelectorPolicy sideSelector;
    final BranchCollisionPolicy collisionPolicy;
    final int maxDepth;

    private BidirectionalTraversalDescriptionImpl( TraversalDescription start,
            TraversalDescription end, BranchCollisionPolicy collisionPolicy, Evaluator collisionEvaluator,
            SideSelectorPolicy sideSelector, int maxDepth )
    {
        this.start = start;
        this.end = end;
        this.collisionPolicy = collisionPolicy;
        this.collisionEvaluator = collisionEvaluator;
        this.sideSelector = sideSelector;
        this.maxDepth = maxDepth;
    }
    
    public BidirectionalTraversalDescriptionImpl()
    {
        // TODO Proper defaults.
        this( traversal(), traversal(), BranchCollisionPolicies.STANDARD, Evaluators.all(), SideSelectorPolicies.ALTERNATING,
                Integer.MAX_VALUE );
    }
    
    @Override
    public BidirectionalTraversalDescription startSide( TraversalDescription startSideDescription )
    {
        return new BidirectionalTraversalDescriptionImpl( startSideDescription, this.end, this.collisionPolicy,
                this.collisionEvaluator, this.sideSelector, this.maxDepth );
    }

    @Override
    public BidirectionalTraversalDescription endSide( TraversalDescription endSideDescription )
    {
        return new BidirectionalTraversalDescriptionImpl( this.start, endSideDescription,
                this.collisionPolicy, this.collisionEvaluator, this.sideSelector, this.maxDepth );
    }
    
    @Override
    public BidirectionalTraversalDescription mirroredSides( TraversalDescription sideDescription )
    {
        return new BidirectionalTraversalDescriptionImpl( sideDescription, sideDescription.reverse(),
                collisionPolicy, collisionEvaluator, sideSelector, maxDepth );
    }
    
    @Override
    public BidirectionalTraversalDescription collisionPolicy( BranchCollisionPolicy collisionPolicy )
    {
        return new BidirectionalTraversalDescriptionImpl( this.start, this.end,
                collisionPolicy, this.collisionEvaluator, this.sideSelector, this.maxDepth );
    }

    @Override
    public BidirectionalTraversalDescription collisionEvaluator( Evaluator collisionEvaluator )
    {
        nullCheck( collisionEvaluator, Evaluator.class, "RETURN_ALL" );
        return new BidirectionalTraversalDescriptionImpl( this.start, this.end, this.collisionPolicy,
                addEvaluator( this.collisionEvaluator, collisionEvaluator ), this.sideSelector, maxDepth );
    }

    @Override
    public BidirectionalTraversalDescription sideSelector( SideSelectorPolicy sideSelector, int maxDepth )
    {
        return new BidirectionalTraversalDescriptionImpl( this.start, this.end, this.collisionPolicy,
                this.collisionEvaluator, sideSelector, maxDepth );
    }

    @Override
    public Traverser traverse( Node start, Node end )
    {
        return new BidirectionalTraverserImpl( this, Arrays.asList( start ), Arrays.asList( end ) );
    }

    @Override
    public Traverser traverse( Iterable<Node> start, Iterable<Node> end )
    {
        return new BidirectionalTraverserImpl( this, start, end );
    }
}
