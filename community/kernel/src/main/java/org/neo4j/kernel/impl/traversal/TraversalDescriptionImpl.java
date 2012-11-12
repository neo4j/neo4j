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

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialStateFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.kernel.StandardExpander;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public final class TraversalDescriptionImpl implements TraversalDescription
{
    public TraversalDescriptionImpl()
    {
        this( Traversal.emptyPathExpander(), Uniqueness.NODE_GLOBAL, null,
                Evaluators.all(), InitialStateFactory.NO_STATE, Traversal.preorderDepthFirst(), null, null );
    }

    final PathExpander expander;
    final InitialStateFactory initialState;
    final UniquenessFactory uniqueness;
    final Object uniquenessParameter;
    final Evaluator evaluator;
    final BranchOrderingPolicy branchOrdering;
    final Comparator<? super Path> sorting;
    final Collection<Node> endNodes;

    private TraversalDescriptionImpl( PathExpander expander,
            UniquenessFactory uniqueness, Object uniquenessParameter,
            Evaluator evaluator, InitialStateFactory<?> initialState, BranchOrderingPolicy branchOrdering,
            Comparator<? super Path> sorting, Collection<Node> endNodes )
    {
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.evaluator = evaluator;
        this.branchOrdering = branchOrdering;
        this.sorting = sorting;
        this.endNodes = endNodes;
        this.initialState = initialState;
    }
    
    public Traverser traverse( Node startNode )
    {
        return new TraverserImpl( this, Arrays.asList( startNode ) );
    }

    public Traverser traverse( Node... startNodes )
    {
        return new TraverserImpl( this, Arrays.asList( startNodes ) );
    }

    /* (non-Javadoc)
     * @see org.neo4j.graphdb.traversal.TraversalDescription#uniqueness(org.neo4j.graphdb.traversal.Uniqueness)
     */
    public TraversalDescription uniqueness( UniquenessFactory uniqueness )
    {
        return new TraversalDescriptionImpl( expander, uniqueness, null,
                evaluator, initialState, branchOrdering, sorting, endNodes );
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
                evaluator, initialState, branchOrdering, sorting, endNodes );
    }
    
    public TraversalDescription evaluator( Evaluator evaluator )
    {
        if ( this.evaluator == evaluator )
        {
            return this;
        }
        nullCheck( evaluator, Evaluator.class, "RETURN_ALL" );
        return new TraversalDescriptionImpl( expander, uniqueness, uniquenessParameter,
                addEvaluator( this.evaluator, evaluator ), initialState, branchOrdering, sorting, endNodes );
    }
    
    protected static Evaluator addEvaluator( Evaluator existing, Evaluator toAdd )
    {
        if ( existing instanceof MultiEvaluator )
        {
            return ((MultiEvaluator) existing).add( toAdd );
        }
        else
        {
            return existing == Evaluators.all() ? toAdd :
                new MultiEvaluator( new Evaluator[] { existing, toAdd } );
        }
    }
    
    protected static <T> void nullCheck( T parameter, Class<T> parameterType, String defaultName )
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
    public TraversalDescription order( BranchOrderingPolicy order )
    {
        if ( this.branchOrdering == order )
        {
            return this;
        }
        return new TraversalDescriptionImpl( expander, uniqueness, uniquenessParameter,
                evaluator, initialState, order, sorting, endNodes );
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
        if ( expander instanceof Expander )
            return expand( ((Expander)expander).add( type, direction ) );
        throw new IllegalStateException( "The current expander cannot be added to" );
    }
    
    public TraversalDescription expand( RelationshipExpander expander )
    {
        return expand( StandardExpander.toPathExpander( expander ) );
    }

    public TraversalDescription expand( PathExpander<?> expander )
    {
        if ( expander.equals( this.expander ) )
        {
            return this;
        }
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, evaluator, initialState, branchOrdering, sorting, endNodes );
    }
    
    public <STATE> TraversalDescription expand( PathExpander<STATE> expander, InitialStateFactory<STATE> initialState )
    {
        return new TraversalDescriptionImpl( expander, uniqueness,
                uniquenessParameter, evaluator, initialState, branchOrdering, sorting, endNodes );
    }
    
    @Override
    public TraversalDescription sort( Comparator<? super Path> sorting )
    {
        return new TraversalDescriptionImpl( expander, uniqueness, uniquenessParameter, evaluator,
                initialState, branchOrdering, sorting, endNodes );
    }
    
    @Override
    public TraversalDescription reverse()
    {
        return new TraversalDescriptionImpl( expander.reverse(), uniqueness, uniquenessParameter,
                evaluator, initialState, branchOrdering, sorting, endNodes );
    }
}
