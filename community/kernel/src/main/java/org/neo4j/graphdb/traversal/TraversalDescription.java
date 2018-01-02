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

import java.util.Comparator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Uniqueness;

/**
 * Represents a description of a traversal. This interface describes the rules
 * and behavior of a traversal. A traversal description is immutable and each
 * method which adds or modifies the behavior returns a new instances that
 * includes the new modification, leaving the instance which returns the new
 * instance intact. For instance,
 *
 * <pre>
 * TraversalDescription td = new TraversalDescriptionImpl();
 * td.depthFirst();
 * </pre>
 *
 * is not going to modify td. you will need to reassign td, like
 *
 * <pre>
 * td = td.depthFirst();
 * </pre>
 * <p>
 * When all the rules and behaviors have been described the traversal is started
 * by using {@link #traverse(Node)} where a starting node is supplied. The
 * {@link Traverser} that is returned is then used to step through the graph,
 * and return the positions that matches the rules.
 */
public interface TraversalDescription
{
    /**
     * Sets the {@link UniquenessFactory} for creating the
     * {@link UniquenessFilter} to use.
     *
     * @param uniqueness the {@link UniquenessFactory} the creator
     * of the desired {@link UniquenessFilter} to use.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription uniqueness( UniquenessFactory uniqueness );

    /**
     * Sets the {@link UniquenessFactory} for creating the
     * {@link UniquenessFilter} to use. It also accepts an extra parameter
     * which is mandatory for certain uniquenesses, f.ex
     * {@link Uniqueness#NODE_RECENT}.
     *
     * @param uniqueness the {@link UniquenessFactory} the creator
     * of the desired {@link UniquenessFilter} to use.
     * @param parameter an extra parameter 
     * which is mandatory for certain uniquenesses, f.ex
     * {@link Uniqueness#NODE_RECENT}.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription uniqueness( UniquenessFactory uniqueness, Object parameter );

    /**
     * Adds {@code evaluator} to the list of evaluators which will control the
     * behavior of the traversal. Each {@link Evaluator} can decide whether or
     * not to include a position in the traverser result, i.e. return it from
     * the {@link Traverser} iterator and also whether to continue down that
     * path or to prune, so that the traverser won't continue further down that
     * path.
     *
     * Multiple {@link Evaluator}s can be added. For a path to be included in
     * the result, all evaluators must agree to include it, i.e. returning
     * either {@link Evaluation#INCLUDE_AND_CONTINUE} or
     * {@link Evaluation#INCLUDE_AND_PRUNE}. For making the traversal continue
     * down that path all evaluators must agree to continue from that path, i.e.
     * returning either {@link Evaluation#INCLUDE_AND_CONTINUE} or
     * {@link Evaluation#EXCLUDE_AND_CONTINUE}.
     *
     * @param evaluator the {@link Evaluator} to add to the traversal
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription evaluator( Evaluator evaluator );

    /**
     * Adds {@code evaluator} to the list of evaluators which will control the
     * behavior of the traversal. Each {@link PathEvaluator} can decide whether or
     * not to include a position in the traverser result, i.e. return it from
     * the {@link Traverser} iterator and also whether to continue down that
     * path or to prune, so that the traverser won't continue further down that
     * path.
     *
     * Multiple {@link PathEvaluator}s can be added. For a path to be included in
     * the result, all evaluators must agree to include it, i.e. returning
     * either {@link Evaluation#INCLUDE_AND_CONTINUE} or
     * {@link Evaluation#INCLUDE_AND_PRUNE}. For making the traversal continue
     * down that path all evaluators must agree to continue from that path, i.e.
     * returning either {@link Evaluation#INCLUDE_AND_CONTINUE} or
     * {@link Evaluation#EXCLUDE_AND_CONTINUE}.
     *
     * @param evaluator the {@link PathEvaluator} to add to the traversal
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription evaluator( PathEvaluator evaluator );

    /**
     * Sets the {@link BranchOrderingPolicy} to use. A {@link BranchSelector}
     * is the basic decisions in the traversal of "where to go next".
     * Examples of default implementations are "breadth first" and
     * "depth first", which can be set with convenience methods
     * {@link #breadthFirst()} and {@link #depthFirst()}.
     *
     * @param selector the factory which creates the {@link BranchSelector}
     * to use with the traversal.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription order( BranchOrderingPolicy selector );

    /**
     * A convenience method for {@link #order(BranchOrderingPolicy)}
     * where a "preorder depth first" selector is used. Positions which are
     * deeper than the current position will be returned before positions on
     * the same depth. See http://en.wikipedia.org/wiki/Depth-first_search
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription depthFirst();

    /**
     * A convenience method for {@link #order(BranchOrderingPolicy)}
     * where a "preorder breadth first" selector is used. All positions with
     * the same depth will be returned before advancing to the next depth.
     * See http://en.wikipedia.org/wiki/Breadth-first_search
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription breadthFirst();

    /**
     * Adds {@code type} to the list of relationship types to traverse.
     * There's no priority or order in which types to traverse.
     *
     * @param type the {@link RelationshipType} to add to the list of types
     * to traverse.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription relationships( RelationshipType type );

    /**
     * Adds {@code type} to the list of relationship types to traverse in
     * the given {@code direction}. There's no priority or order in which
     * types to traverse.
     *
     * @param type the {@link RelationshipType} to add to the list of types
     * to traverse.
     * @param direction the {@link Direction} to traverse this type of
     * relationship in.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription relationships( RelationshipType type,
            Direction direction );

    /**
     * Sets the {@link PathExpander} as the expander of relationships,
     * discarding all previous calls to
     * {@link #relationships(RelationshipType)} and
     * {@link #relationships(RelationshipType, Direction)} or any other expand method.
     *
     * @param expander the {@link PathExpander} to use.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription expand( PathExpander<?> expander );
    
    /**
     * Sets the {@link PathExpander} as the expander of relationships,
     * discarding all previous calls to
     * {@link #relationships(RelationshipType)} and
     * {@link #relationships(RelationshipType, Direction)} or any other expand method.
     * The supplied {@link InitialStateFactory} will provide the initial traversal branches
     * with state values which flows down throughout the traversal and can be changed
     * for child branches by the {@link PathExpander} at any level.
     *
     * @param expander the {@link PathExpander} to use.
     * @param initialState factory for supplying the initial traversal branches with
     * state values potentially used by the {@link PathExpander}.
     * @param <STATE> the type of the state object
     * @return a new traversal description with the new modifications.
     *
     * @deprecated Because InitialStateFactory is deprecated
     */
    <STATE> TraversalDescription expand( PathExpander<STATE> expander, InitialStateFactory<STATE> initialState );
    
    /**
     * Sets the {@link PathExpander} as the expander of relationships,
     * discarding all previous calls to
     * {@link #relationships(RelationshipType)} and
     * {@link #relationships(RelationshipType, Direction)} or any other expand method.
     * The supplied {@link InitialBranchState} will provide the initial traversal branches
     * with state values which flows down throughout the traversal and can be changed
     * for child branches by the {@link PathExpander} at any level.
     *
     * @param expander the {@link PathExpander} to use.
     * @param initialState factory for supplying the initial traversal branches with
     * state values potentially used by the {@link PathExpander}.
     * @param <STATE> the type of the state object
     * @return a new traversal description with the new modifications.
     */
    <STATE> TraversalDescription expand( PathExpander<STATE> expander, InitialBranchState<STATE> initialState );
    
    /**
     * Sets the {@link RelationshipExpander} as the expander of relationships,
     * discarding all previous calls to
     * {@link #relationships(RelationshipType)} and
     * {@link #relationships(RelationshipType, Direction)} or any other expand method.
     *
     * @param expander the {@link RelationshipExpander} to use.
     * @return a new traversal description with the new modifications.
     *
     * @deprecated Because RelationshipExpander is deprecated
     */
    TraversalDescription expand( RelationshipExpander expander );
    
    /**
     * @param comparator the {@link Comparator} to use for sorting the paths.
     * @return the paths from this traversal sorted according to {@code comparator}.
     */
    TraversalDescription sort( Comparator<? super Path> comparator );
    
    /**
     * Creates an identical {@link TraversalDescription}, although reversed in
     * how it traverses the graph.
     * 
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription reverse();
    
    /**
     * Traverse from a single start node based on all the rules and behavior
     * in this description. A {@link Traverser} is returned which is
     * used to step through the graph and getting results back. The traversal
     * is not guaranteed to start before the Traverser is used.
     *
     * @param startNode {@link Node} to start traversing from.
     * @return a {@link Traverser} used to step through the graph and to get
     * results from.
     */
    Traverser traverse( Node startNode );

    /**
     * Traverse from a set of start nodes based on all the rules and behavior
     * in this description. A {@link Traverser} is returned which is
     * used to step through the graph and getting results back. The traversal
     * is not guaranteed to start before the Traverser is used.
     *
     * @param startNodes {@link Node}s to start traversing from.
     * @return a {@link Traverser} used to step through the graph and to get
     * results from.
     */
    Traverser traverse( Node... startNodes );

    /**
     * Traverse from a iterable of start nodes based on all the rules and behavior
     * in this description. A {@link Traverser} is returned which is
     * used to step through the graph and getting results back. The traversal
     * is not guaranteed to start before the Traverser is used.
     *
     * @param iterableStartNodes {@link Node}s to start traversing from.
     * @return a {@link Traverser} used to step through the graph and to get
     * results from.
     */
    Traverser traverse( final Iterable<Node> iterableStartNodes );
}
