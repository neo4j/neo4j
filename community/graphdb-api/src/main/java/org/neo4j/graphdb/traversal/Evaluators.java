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
package org.neo4j.graphdb.traversal;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import static java.util.Arrays.asList;

import static org.neo4j.graphdb.traversal.Evaluation.INCLUDE_AND_CONTINUE;

/**
 * Common {@link Evaluator}s useful during common traversals.
 *
 * @see Evaluator
 * @see TraversalDescription
 */
public abstract class Evaluators
{
    @SuppressWarnings( "rawtypes" )
    private static final PathEvaluator<?> ALL = new PathEvaluator.Adapter()
    {
        @Override
        public Evaluation evaluate( Path path, BranchState state )
        {
            return INCLUDE_AND_CONTINUE;
        }
    };

    private static final PathEvaluator ALL_BUT_START_POSITION = fromDepth( 1 );

    /**
     * @param <STATE> the type of the state object.
     * @return an evaluator which includes everything it encounters and doesn't prune
     *         anything.
     */
    public static <STATE> PathEvaluator<STATE> all()
    {
        //noinspection unchecked
        return (PathEvaluator<STATE>) ALL;
    }

    /**
     * @return an evaluator which never prunes and includes everything except
     *         the first position, i.e. the the start node.
     */
    public static PathEvaluator excludeStartPosition()
    {
        return ALL_BUT_START_POSITION;
    }

    /**
     * Returns an {@link Evaluator} which includes positions down to {@code depth}
     * and prunes everything deeper than that.
     *
     * @param depth   the max depth to traverse to.
     * @param <STATE> the type of the state object.
     * @return Returns an {@link Evaluator} which includes positions down to
     *         {@code depth} and prunes everything deeper than that.
     */
    public static <STATE> PathEvaluator<STATE> toDepth( final int depth )
    {
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                int pathLength = path.length();
                return Evaluation.of( pathLength <= depth, pathLength < depth );
            }
        };
    }

    /**
     * Returns an {@link Evaluator} which only includes positions from {@code depth}
     * and deeper and never prunes anything.
     *
     * @param depth   the depth to start include positions from.
     * @param <STATE> the type of the state object.
     * @return Returns an {@link Evaluator} which only includes positions from
     *         {@code depth} and deeper and never prunes anything.
     */
    public static <STATE> PathEvaluator<STATE> fromDepth( final int depth )
    {
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                return Evaluation.ofIncludes( path.length() >= depth );
            }
        };
    }

    /**
     * Returns an {@link Evaluator} which only includes positions at {@code depth}
     * and prunes everything deeper than that.
     *
     * @param depth   the depth to start include positions from.
     * @param <STATE> the type of the state object.
     * @return Returns an {@link Evaluator} which only includes positions at
     *         {@code depth} and prunes everything deeper than that.
     */
    public static <STATE> PathEvaluator<STATE> atDepth( final int depth )
    {
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                return path.length() == depth ? Evaluation.INCLUDE_AND_PRUNE : Evaluation.EXCLUDE_AND_CONTINUE;
            }
        };
    }

    /**
     * Returns an {@link Evaluator} which only includes positions between
     * depths {@code minDepth} and {@code maxDepth}. It prunes everything deeper
     * than {@code maxDepth}.
     *
     * @param minDepth minimum depth a position must have to be included.
     * @param maxDepth maximum depth a position must have to be included.
     * @param <STATE>  the type of the state object.
     * @return Returns an {@link Evaluator} which only includes positions between
     *         depths {@code minDepth} and {@code maxDepth}. It prunes everything deeper
     *         than {@code maxDepth}.
     */
    public static <STATE> PathEvaluator<STATE> includingDepths( final int minDepth, final int maxDepth )
    {
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                int length = path.length();
                return Evaluation.of( length >= minDepth && length <= maxDepth, length < maxDepth );
            }
        };
    }

    /**
     * Returns an {@link Evaluator} which compares the type of the last relationship
     * in a {@link Path} to a given set of relationship types (one or more).If the type of
     * the last relationship in a path is of one of the given types then
     * {@code evaluationIfMatch} will be returned, otherwise
     * {@code evaluationIfNoMatch} will be returned.
     *
     * @param evaluationIfMatch   the {@link Evaluation} to return if the type of the
     *                            last relationship in the path matches any of the given types.
     * @param evaluationIfNoMatch the {@link Evaluation} to return if the type of the
     *                            last relationship in the path doesn't match any of the given types.
     * @param type                the (first) type (of possibly many) to match the last relationship
     *                            in paths with.
     * @param orAnyOfTheseTypes   additional types to match the last relationship in
     *                            paths with.
     * @param <STATE>             the type of the state object.
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     */
    public static <STATE> PathEvaluator<STATE> lastRelationshipTypeIs( final Evaluation evaluationIfMatch,
            final Evaluation evaluationIfNoMatch, final RelationshipType type, RelationshipType... orAnyOfTheseTypes )
    {
        if ( orAnyOfTheseTypes.length == 0 )
        {
            return new PathEvaluator.Adapter<STATE>()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    Relationship rel = path.lastRelationship();
                    return rel != null && rel.isType( type ) ? evaluationIfMatch : evaluationIfNoMatch;
                }
            };
        }

        final Set<String> expectedTypes = new HashSet<>();
        expectedTypes.add( type.name() );
        for ( RelationshipType otherType : orAnyOfTheseTypes )
        {
            expectedTypes.add( otherType.name() );
        }

        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                Relationship lastRelationship = path.lastRelationship();
                if ( lastRelationship == null )
                {
                    return evaluationIfNoMatch;
                }

                return expectedTypes.contains( lastRelationship.getType().name() ) ?
                        evaluationIfMatch : evaluationIfNoMatch;
            }
        };
    }

    /**
     * @param type              the (first) type (of possibly many) to match the last relationship
     *                          in paths with.
     * @param orAnyOfTheseTypes types to match the last relationship in paths with. If any matches
     *                          it's considered a match.
     * @param <STATE>           the type of the state object.
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     * @see #lastRelationshipTypeIs(Evaluation, Evaluation, RelationshipType, RelationshipType...)
     *      Uses {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfMatch}
     *      and {@link Evaluation#EXCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     */
    public static <STATE> PathEvaluator<STATE> includeWhereLastRelationshipTypeIs( RelationshipType type,
            RelationshipType... orAnyOfTheseTypes )
    {
        return lastRelationshipTypeIs( Evaluation.INCLUDE_AND_CONTINUE, Evaluation.EXCLUDE_AND_CONTINUE,
                type, orAnyOfTheseTypes );
    }

    /**
     * @param type              the (first) type (of possibly many) to match the last relationship
     *                          in paths with.
     * @param orAnyOfTheseTypes types to match the last relationship in paths with. If any matches
     *                          it's considered a match.
     * @param <STATE>           the type of the state object.
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     * @see #lastRelationshipTypeIs(Evaluation, Evaluation, RelationshipType, RelationshipType...)
     *      Uses {@link Evaluation#INCLUDE_AND_PRUNE} for {@code evaluationIfMatch}
     *      and {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     */
    public static <STATE> PathEvaluator<STATE> pruneWhereLastRelationshipTypeIs( RelationshipType type,
            RelationshipType... orAnyOfTheseTypes )
    {
        return lastRelationshipTypeIs( Evaluation.INCLUDE_AND_PRUNE, Evaluation.EXCLUDE_AND_CONTINUE,
                type, orAnyOfTheseTypes );
    }

    /**
     * An {@link Evaluator} which will return {@code evaluationIfMatch} if {@link Path#endNode()}
     * for a given path is any of {@code nodes}, else {@code evaluationIfNoMatch}.
     *
     * @param evaluationIfMatch   the {@link Evaluation} to return if the {@link Path#endNode()}
     *                            is any of the nodes in {@code nodes}.
     * @param evaluationIfNoMatch the {@link Evaluation} to return if the {@link Path#endNode()}
     *                            doesn't match any of the nodes in {@code nodes}.
     * @param possibleEndNodes    a set of nodes to match to end nodes in paths.
     * @param <STATE>             the type of the state object.
     * @return an {@link Evaluator} which will return {@code evaluationIfMatch} if
     *         {@link Path#endNode()} for a given path is any of {@code nodes},
     *         else {@code evaluationIfNoMatch}.
     */
    public static <STATE> PathEvaluator<STATE> endNodeIs( final Evaluation evaluationIfMatch, final Evaluation
        evaluationIfNoMatch,
            Node... possibleEndNodes )
    {
        if ( possibleEndNodes.length == 1 )
        {
            final Node target = possibleEndNodes[0];
            return new PathEvaluator.Adapter<STATE>()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    return target.equals( path.endNode() ) ? evaluationIfMatch : evaluationIfNoMatch;
                }
            };
        }

        final Set<Node> endNodes = new HashSet<>( asList( possibleEndNodes ) );
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                return endNodes.contains( path.endNode() ) ? evaluationIfMatch : evaluationIfNoMatch;
            }
        };
    }

    /**
     * Include paths with the specified end nodes.
     *
     * Uses Evaluators#endNodeIs(Evaluation, Evaluation, Node...) with
     * {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfMatch} and
     * {@link Evaluation#EXCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     *
     * @param nodes   end nodes for paths to be included in the result.
     * @param <STATE> the type of the state object.
     * @return paths where the end node is one of {@code nodes}
     */
    public static <STATE> PathEvaluator<STATE> includeWhereEndNodeIs( Node... nodes )
    {
        return endNodeIs( Evaluation.INCLUDE_AND_CONTINUE, Evaluation.EXCLUDE_AND_CONTINUE, nodes );
    }

    public static <STATE> PathEvaluator<STATE> pruneWhereEndNodeIs( Node... nodes )
    {
        return endNodeIs( Evaluation.INCLUDE_AND_PRUNE, Evaluation.EXCLUDE_AND_CONTINUE, nodes );
    }

    /**
     * Evaluator which decides to include a {@link Path} if all the
     * {@code nodes} exist in it.
     *
     * @param nodes   {@link Node}s that must exist in a {@link Path} for it
     *                to be included.
     * @param <STATE> the type of the state object.
     * @return {@link Evaluation#INCLUDE_AND_CONTINUE} if all {@code nodes}
     *         exist in a given {@link Path}, otherwise
     *         {@link Evaluation#EXCLUDE_AND_CONTINUE}.
     */
    public static <STATE> PathEvaluator<STATE> includeIfContainsAll( final Node... nodes )
    {
        if ( nodes.length == 1 )
        {
            return new PathEvaluator.Adapter<STATE>()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    for ( Node node : path.reverseNodes() )
                    {
                        if ( node.equals( nodes[0] ) )
                        {
                            return Evaluation.INCLUDE_AND_CONTINUE;
                        }
                    }
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }
            };
        }

        final Set<Node> fullSet = new HashSet<>( asList( nodes ) );
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                Set<Node> set = new HashSet<>( fullSet );
                for ( Node node : path.reverseNodes() )
                {
                    if ( set.remove( node ) && set.isEmpty() )
                    {
                        return Evaluation.INCLUDE_AND_CONTINUE;
                    }
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        };
    }

    /**
     * Whereas adding {@link Evaluator}s to a {@link TraversalDescription} puts those
     * evaluators in {@code AND-mode} this can group many evaluators in {@code OR-mode}.
     *
     * @param evaluators represented as one evaluators. If any of the evaluators decides
     *                   to include a path it will be included.
     * @param <STATE>    the type of the state object.
     * @return an {@link Evaluator} which decides to include a path if any of the supplied
     *         evaluators wants to include it.
     */
    public static <STATE> PathEvaluator<STATE> includeIfAcceptedByAny( final PathEvaluator... evaluators )
    {
        return new PathEvaluator.Adapter<STATE>()
        {
            @SuppressWarnings( "unchecked" )
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                for ( PathEvaluator evaluator : evaluators )
                {
                    if ( evaluator.evaluate( path, state ).includes() )
                    {
                        return Evaluation.INCLUDE_AND_CONTINUE;
                    }
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        };
    }

    /**
     * Whereas adding {@link Evaluator}s to a {@link TraversalDescription} puts those
     * evaluators in {@code AND-mode} this can group many evaluators in {@code OR-mode}.
     *
     * @param evaluators represented as one evaluators. If any of the evaluators decides
     *                   to include a path it will be included.
     * @param <STATE>    the type of the state object.
     * @return an {@link Evaluator} which decides to include a path if any of the supplied
     *         evaluators wants to include it.
     */
    public static <STATE> PathEvaluator<STATE> includeIfAcceptedByAny( final Evaluator... evaluators )
    {
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                for ( Evaluator evaluator : evaluators )
                {
                    if ( evaluator.evaluate( path ).includes() )
                    {
                        return Evaluation.INCLUDE_AND_CONTINUE;
                    }
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        };
    }

    /**
     * Returns {@link Evaluator}s for paths with the specified depth and with an end node from the list of
     * possibleEndNodes.
     * @param depth The exact depth to filter the returned path evaluators.
     * @param possibleEndNodes Filter for the possible nodes to end the path on.
     * @param <STATE> the type of the state object.
     * @return {@link Evaluator}s for paths with the specified depth and with an end node from the list of
     * possibleEndNodes.
     */
    public static <STATE> PathEvaluator<STATE> endNodeIsAtDepth( final int depth, Node... possibleEndNodes )
    {
        if ( possibleEndNodes.length == 1 )
        {
            final Node target = possibleEndNodes[0];
            return new PathEvaluator.Adapter<STATE>()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    if ( path.length() == depth )
                    {
                        return target.equals( path.endNode() ) ? Evaluation.INCLUDE_AND_PRUNE : Evaluation.EXCLUDE_AND_PRUNE;
                    }
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }
            };
        }

        final Set<Node> endNodes = new HashSet<>( asList( possibleEndNodes ) );
        return new PathEvaluator.Adapter<STATE>()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                if ( path.length() == depth )
                {
                    return endNodes.contains( path.endNode() ) ? Evaluation.INCLUDE_AND_PRUNE : Evaluation.EXCLUDE_AND_PRUNE;
                }
                return Evaluation.EXCLUDE_AND_CONTINUE;
            }
        };
    }
}
