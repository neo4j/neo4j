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

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public final class TraversalDescriptionImpl implements TraversalDescription
{
    public TraversalDescriptionImpl()
    {
        this( StandardExpander.DEFAULT, Uniqueness.NODE_GLOBAL, null,
                Evaluators.all(), Traversal.preorderDepthFirst() );
    }

    final Expander expander;
    final UniquenessFactory uniqueness;
    final Object uniquenessParameter;
    final Evaluator evaluator;
    final BranchOrderingPolicy branchSelector;

    private TraversalDescriptionImpl( Expander expander,
            UniquenessFactory uniqueness, Object uniquenessParameter,
            Evaluator evaluator, BranchOrderingPolicy branchSelector )
    {
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.evaluator = evaluator;
        this.branchSelector = branchSelector;
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#traverse(org.neo4j.graphdb.Node)
     */
    public Traverser traverse( Node startNode )
    {
        return new TraverserImpl( this, startNode );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness)
     */
    public TraversalDescription uniqueness( UniquenessFactory uniqueness )
    {
        return new TraversalDescriptionImpl( expander, uniqueness, null,
                evaluator, branchSelector );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness, java.lang.Object)
     */
    public TraversalDescription uniqueness( UniquenessFactory uniqueness,
            Object parameter )
    {
        if ( this.uniqueness == uniqueness )
        {
            if ( uniquenessParameter == null ? parameter == null
                    : uniquenessParameter.equals( parameter ) )
            {
                return this;
            }
        }

        return new TraversalDescriptionImpl( expander, uniqueness, parameter,
                evaluator, branchSelector );
    }
    
    public TraversalDescription prune( PruneEvaluator pruning )
    {
        return evaluator( pruning == PruneEvaluator.NONE ? Evaluators.all() :
                new WrappedPruneEvaluator( pruning ) );
    }
    
    public TraversalDescription filter( Predicate<Path> filter )
    {
        Evaluator evaluator = null;
        if ( filter == Traversal.returnAll() )
        {
            evaluator = Evaluators.all();
        }
        else if ( filter == Traversal.returnAllButStartNode() )
        {
            evaluator = Evaluators.excludeStartPosition();
        }
        else
        {
            evaluator = new WrappedFilter( filter );
        }
        return evaluator( evaluator );
    }
    
    public TraversalDescription evaluator( Evaluator evaluator )
    {
        if ( this.evaluator == evaluator )
        {
            return this;
        }
        nullCheck( evaluator, Evaluator.class, "RETURN_ALL" );
        return new TraversalDescriptionImpl( expander, uniqueness, uniquenessParameter,
                addBlaEvaluator( evaluator ), branchSelector );
    }
    
    private Evaluator addBlaEvaluator( Evaluator evaluator )
    {
        if ( this.evaluator instanceof MultiEvaluator )
        {
            return ((MultiEvaluator) this.evaluator).add( evaluator );
        }
        else
        {
            if ( this.evaluator == Evaluators.all() )
            {
                return evaluator;
            }
            else
            {
                return new MultiEvaluator( new Evaluator[] { this.evaluator, evaluator } );
            }
        }
    }
    
    private static <T> void nullCheck( T parameter, Class<T> parameterType,
            String defaultName )
    {
        if ( parameter == null )
        {
            String typeName = parameterType.getSimpleName();
            throw new IllegalArgumentException( typeName
                                                + " may not be null, use "
                                                + typeName + "." + defaultName
                                                + " instead." );
        }
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#order(org.neo4j.graphdb.traversal.Order)
     */
    public TraversalDescription order( BranchOrderingPolicy selector )
    {
        if ( this.branchSelector == selector )
        {
            return this;
        }
        return new TraversalDescriptionImpl( expander, uniqueness, uniquenessParameter,
                evaluator, selector );
    }

    public TraversalDescription depthFirst()
    {
        return order( Traversal.preorderDepthFirst() );
    }

    public TraversalDescription breadthFirst()
    {
        return order( Traversal.preorderBreadthFirst() );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#relationships(org.neo4j.graphdb.RelationshipType)
     */
    public TraversalDescription relationships( RelationshipType type )
    {
        return relationships( type, Direction.BOTH );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#relationships(org.neo4j.graphdb.RelationshipType, org.neo4j.graphdb.Direction)
     */
    public TraversalDescription relationships( RelationshipType type,
            Direction direction )
    {
        return expand( expander.add( type, direction ) );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#expand(org.neo4j.graphdb.RelationshipExpander)
     */
    public TraversalDescription expand(RelationshipExpander expander)
    {
        if ( expander.equals( this.expander ) )
        {
            return this;
        }
        return new TraversalDescriptionImpl( Traversal.expander( expander ), uniqueness,
                uniquenessParameter, evaluator, branchSelector );
    }
    
    private static class WrappedPruneEvaluator implements Evaluator
    {
        private final PruneEvaluator pruning;

        WrappedPruneEvaluator( PruneEvaluator pruning )
        {
            this.pruning = pruning;
        }

        public Evaluation evaluate( Path path )
        {
            // Before the Evaluator, when PruneEvaluator was used individually a PruneEvaluator
            // was never called with the start node as argument. This condition mimics that behaviour.
            if ( path.length() == 0 )
            {
                return Evaluation.INCLUDE_AND_CONTINUE;
            }
            return pruning.pruneAfter( path ) ? Evaluation.INCLUDE_AND_PRUNE : Evaluation.INCLUDE_AND_CONTINUE;
        }
    }
    
    private static class WrappedFilter implements Evaluator
    {
        private final Predicate<Path> filter;

        WrappedFilter( Predicate<Path> filter )
        {
            this.filter = filter;
        }

        public Evaluation evaluate( Path path )
        {
            return filter.accept( path ) ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_CONTINUE;
        }
    }
}
