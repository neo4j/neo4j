/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanderBuilder;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.traversal.BranchOrderingPolicies;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialBranchState;
import org.neo4j.graphdb.traversal.InitialStateFactory;
import org.neo4j.graphdb.traversal.PathEvaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.helpers.Factory;
import org.neo4j.helpers.Provider;
import org.neo4j.kernel.Traversal;

public final class MonoDirectionalTraversalDescription implements TraversalDescription
{
    final static Provider<Resource> NO_STATEMENT = new Provider<Resource>()
    {
        @Override
        public Resource instance()
        {
            return Resource.EMPTY;
        }
    };

    final PathExpanderBuilder expanderBuilder;
    final PathExpander expander;
    final InitialBranchState initialState;
    final Provider<? extends Resource> statementFactory;
    final UniquenessFactory uniqueness;
    final Object uniquenessParameter;
    final PathEvaluator evaluator;
    final BranchOrderingPolicy branchOrdering;
    final Comparator<? super Path> sorting;
    final Collection<Node> endNodes;

    public MonoDirectionalTraversalDescription()
    {
        /*
         * Use one statement per operation performed, rather than a global statement for the whole traversal. This is
         * significantly less performant, and only used when accessing the traversal framework via the legacy access
         * methods (eg. Traversal.description()).
         */
        this(NO_STATEMENT);
    }

    public MonoDirectionalTraversalDescription( Provider<? extends Resource> statementProvider )
    {
        this( PathExpanderBuilder.empty(), null, Uniqueness.NODE_GLOBAL, null,
                Evaluators.all(), InitialBranchState.NO_STATE, Traversal.preorderDepthFirst(), null, null,
                statementProvider );
    }

    private MonoDirectionalTraversalDescription( PathExpanderBuilder expanderBuilder,
                                                 PathExpander expander,
                                                 UniquenessFactory uniqueness, Object uniquenessParameter,
                                                 PathEvaluator evaluator, InitialBranchState initialState,
                                                 BranchOrderingPolicy branchOrdering,
                                                 Comparator<? super Path> sorting, Collection<Node> endNodes,
                                                 Provider<? extends Resource> statementFactory )
    {
        this.expanderBuilder = expanderBuilder;
        this.expander = expander;
        this.uniqueness = uniqueness;
        this.uniquenessParameter = uniquenessParameter;
        this.evaluator = evaluator;
        this.branchOrdering = branchOrdering;
        this.sorting = sorting;
        this.endNodes = endNodes;
        this.initialState = initialState;
        this.statementFactory = statementFactory;
    }

    @Override
    public Traverser traverse( Node startNode )
    {
        return traverse( new Node[]{startNode} );
    }

    @Override
    public Traverser traverse( Node... startNodes )
    {
        final Iterable<Node> iterableStartNodes = Arrays.asList( startNodes );
        return new DefaultTraverser( new Factory<TraverserIterator>(){
            @Override
            public TraverserIterator newInstance()
            {
                Resource statement = statementFactory.instance();
                MonoDirectionalTraverserIterator iterator = new MonoDirectionalTraverserIterator(
                        statement,
                        uniqueness.create( uniquenessParameter ),
                        expander, branchOrdering, evaluator,
                        iterableStartNodes, initialState );
                return sorting != null ? new SortingTraverserIterator( statement, sorting, iterator ) : iterator;
            }
        });
    }

    @Override
    public TraversalDescription uniqueness( UniquenessFactory uniqueness )
    {
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness, null,
                evaluator, initialState, branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public TraversalDescription uniqueness( UniquenessFactory uniqueness,
            Object parameter )
    {
        if ( this.uniqueness == uniqueness &&
             (uniquenessParameter == null ? parameter == null : uniquenessParameter.equals( parameter )) )
        {
            return this;
        }

        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness, parameter,
                evaluator, initialState, branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public TraversalDescription evaluator( Evaluator evaluator )
    {
        return evaluator( new Evaluator.AsPathEvaluator( evaluator) );
    }

    @Override
    public TraversalDescription evaluator( PathEvaluator evaluator )
    {
        if ( this.evaluator == evaluator )
        {
            return this;
        }
        nullCheck( evaluator, Evaluator.class, "RETURN_ALL" );
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness, uniquenessParameter,
                addEvaluator( this.evaluator, evaluator ), initialState, branchOrdering, sorting, endNodes,
                statementFactory );
    }

    protected static PathEvaluator addEvaluator( PathEvaluator existing, PathEvaluator toAdd )
    {
        if ( existing instanceof MultiEvaluator )
        {
            return ((MultiEvaluator) existing).add( toAdd );
        }
        else
        {
            return existing == Evaluators.all() ? toAdd :
                new MultiEvaluator( new PathEvaluator[] { existing, toAdd } );
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

    @Override
    public TraversalDescription order( BranchOrderingPolicy order )
    {
        if ( this.branchOrdering == order )
        {
            return this;
        }
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness, uniquenessParameter,
                evaluator, initialState, order, sorting, endNodes, statementFactory );
    }

    @Override
    public TraversalDescription depthFirst()
    {
        return order( BranchOrderingPolicies.PREORDER_DEPTH_FIRST );
    }

    @Override
    public TraversalDescription breadthFirst()
    {
        return order( BranchOrderingPolicies.PREORDER_BREADTH_FIRST );
    }

    @Override
    public TraversalDescription relationships( RelationshipType type )
    {
        return relationships( type, Direction.BOTH );
    }

    @Override
    public TraversalDescription relationships( RelationshipType type, Direction direction )
    {
        if ( expander != null )
        {
            throw new IllegalStateException( "The current expander cannot be added to" );
        }

        return new MonoDirectionalTraversalDescription( expanderBuilder.add( type, direction ), expander, uniqueness,
                uniquenessParameter, evaluator, initialState, branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public TraversalDescription expand( PathExpander<?> expander )
    {
        if ( expander.equals( this.expander ) )
        {
            return this;
        }
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness,
                uniquenessParameter, evaluator, initialState, branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public <STATE> TraversalDescription expand( PathExpander<STATE> expander, InitialBranchState<STATE> initialState )
    {
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness,
                uniquenessParameter, evaluator, initialState, branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public <STATE> TraversalDescription expand( PathExpander<STATE> expander, InitialStateFactory<STATE> initialState )
    {
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness,
                uniquenessParameter, evaluator, new InitialStateFactory.AsInitialBranchState<>( initialState ),
                branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public TraversalDescription sort( Comparator<? super Path> sorting )
    {
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander, uniqueness, uniquenessParameter,
                evaluator, initialState, branchOrdering, sorting, endNodes, statementFactory );
    }

    @Override
    public TraversalDescription reverse()
    {
        return new MonoDirectionalTraversalDescription( expanderBuilder, expander.reverse(), uniqueness,
                uniquenessParameter, evaluator, initialState.reverse(), branchOrdering, sorting, endNodes,
                statementFactory );
    }
}
