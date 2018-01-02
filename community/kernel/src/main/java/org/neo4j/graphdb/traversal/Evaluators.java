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
 * @author Mattias Persson
 * @see Evaluator
 * @see TraversalDescription
 */
public abstract class Evaluators
{
    @SuppressWarnings( "rawtypes" )
    private static final PathEvaluator ALL = new PathEvaluator.Adapter()
    {
        public Evaluation evaluate( Path path, BranchState state )
        {
            return INCLUDE_AND_CONTINUE;
        }
    };

    @SuppressWarnings( "rawtypes" )
    private static final PathEvaluator ALL_BUT_START_POSITION = fromDepth( 1 );

    /**
     * @return an evaluator which includes everything it encounters and doesn't prune
     *         anything.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator all()
    {
        return ALL;
    }

    /**
     * @return an evaluator which never prunes and includes everything except
     *         the first position, i.e. the the start node.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator excludeStartPosition()
    {
        return ALL_BUT_START_POSITION;
    }

    /**
     * Returns an {@link Evaluator} which includes positions down to {@code depth}
     * and prunes everything deeper than that.
     *
     * @param depth the max depth to traverse to.
     * @return Returns an {@link Evaluator} which includes positions down to
     *         {@code depth} and prunes everything deeper than that.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator toDepth( final int depth )
    {
        return new PathEvaluator.Adapter()
        {
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
     * @param depth the depth to start include positions from.
     * @return Returns an {@link Evaluator} which only includes positions from
     *         {@code depth} and deeper and never prunes anything.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator fromDepth( final int depth )
    {
        return new PathEvaluator.Adapter()
        {
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
     * @param depth the depth to start include positions from.
     * @return Returns an {@link Evaluator} which only includes positions at
     *         {@code depth} and prunes everything deeper than that.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator atDepth( final int depth )
    {
        return new PathEvaluator.Adapter()
        {
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
     * @return Returns an {@link Evaluator} which only includes positions between
     *         depths {@code minDepth} and {@code maxDepth}. It prunes everything deeper
     *         than {@code maxDepth}.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator includingDepths( final int minDepth, final int maxDepth )
    {
        return new PathEvaluator.Adapter()
        {
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
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator lastRelationshipTypeIs( final Evaluation evaluationIfMatch,
            final Evaluation evaluationIfNoMatch, final RelationshipType type, RelationshipType... orAnyOfTheseTypes )
    {
        if ( orAnyOfTheseTypes.length == 0 )
        {
            return new PathEvaluator.Adapter()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    Relationship rel = path.lastRelationship();
                    return rel != null && rel.isType( type ) ? evaluationIfMatch : evaluationIfNoMatch;
                }
            };
        }

        final Set<String> expectedTypes = new HashSet<String>();
        expectedTypes.add( type.name() );
        for ( RelationshipType otherType : orAnyOfTheseTypes )
        {
            expectedTypes.add( otherType.name() );
        }

        return new PathEvaluator.Adapter()
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
     * @deprecated use {@link #includeWhereLastRelationshipTypeIs(RelationshipType, RelationshipType...)}
     * @param type              the (first) type (of possibly many) to match the last relationship
     *                          in paths with.
     * @param orAnyOfTheseTypes types to match the last relationship in paths with. If any matches
     *                          it's considered a match.
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     */
    public static Evaluator returnWhereLastRelationshipTypeIs( RelationshipType type,
                                                               RelationshipType... orAnyOfTheseTypes )
    {
        return includeWhereLastRelationshipTypeIs( type, orAnyOfTheseTypes );
    }

    /**
     * @param type              the (first) type (of possibly many) to match the last relationship
     *                          in paths with.
     * @param orAnyOfTheseTypes types to match the last relationship in paths with. If any matches
     *                          it's considered a match.
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     * @see #lastRelationshipTypeIs(Evaluation, Evaluation, RelationshipType, RelationshipType...)
     *      Uses {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfMatch}
     *      and {@link Evaluation#EXCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator includeWhereLastRelationshipTypeIs( RelationshipType type,
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
     * @return an {@link Evaluator} which compares the type of the last relationship
     *         in a {@link Path} to a given set of relationship types.
     * @see #lastRelationshipTypeIs(Evaluation, Evaluation, RelationshipType, RelationshipType...)
     *      Uses {@link Evaluation#INCLUDE_AND_PRUNE} for {@code evaluationIfMatch}
     *      and {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator pruneWhereLastRelationshipTypeIs( RelationshipType type,
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
     * @return an {@link Evaluator} which will return {@code evaluationIfMatch} if
     *         {@link Path#endNode()} for a given path is any of {@code nodes},
     *         else {@code evaluationIfNoMatch}.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator endNodeIs( final Evaluation evaluationIfMatch, final Evaluation evaluationIfNoMatch,
            Node... possibleEndNodes )
    {
        if ( possibleEndNodes.length == 1 )
        {
            final Node target = possibleEndNodes[0];
            return new PathEvaluator.Adapter()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    return target.equals( path.endNode() ) ? evaluationIfMatch : evaluationIfNoMatch;
                }
            };
        }

        final Set<Node> endNodes = new HashSet<Node>( asList( possibleEndNodes ) );
        return new PathEvaluator.Adapter()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                return endNodes.contains( path.endNode() ) ? evaluationIfMatch : evaluationIfNoMatch;
            }
        };
    }

    /**
     * @deprecated use {@link #includeWhereEndNodeIs(Node...)}
     * @param nodes end nodes for paths to be included in the result.
     * @return an {@link Evaluator} which only includes positions where the end node is one of {@code nodes}
     */
    public static Evaluator returnWhereEndNodeIs( Node... nodes )
    {
        return includeWhereEndNodeIs( nodes );
    }

    /**
     * Include paths with the specified end nodes.
     * 
     * Uses Evaluators#endNodeIs(Evaluation, Evaluation, Node...) with
     * {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfMatch} and
     * {@link Evaluation#EXCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     * 
     * @param nodes end nodes for paths to be included in the result.
     * @return paths where the end node is one of {@code nodes}
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator includeWhereEndNodeIs( Node... nodes )
    {
        return endNodeIs( Evaluation.INCLUDE_AND_CONTINUE, Evaluation.EXCLUDE_AND_CONTINUE, nodes );
    }

    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator pruneWhereEndNodeIs( Node... nodes )
    {
        return endNodeIs( Evaluation.INCLUDE_AND_PRUNE, Evaluation.EXCLUDE_AND_CONTINUE, nodes );
    }

    /**
     * Evaluator which decides to include a {@link Path} if all the
     * {@code nodes} exist in it.
     *
     * @param nodes {@link Node}s that must exist in a {@link Path} for it
     *              to be included.
     * @return {@link Evaluation#INCLUDE_AND_CONTINUE} if all {@code nodes}
     *         exist in a given {@link Path}, otherwise
     *         {@link Evaluation#EXCLUDE_AND_CONTINUE}.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator includeIfContainsAll( final Node... nodes )
    {
        if ( nodes.length == 1 )
        {
            return new PathEvaluator.Adapter()
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
        else
        {
            final Set<Node> fullSet = new HashSet<Node>( asList( nodes ) );
            return new PathEvaluator.Adapter()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    Set<Node> set = new HashSet<Node>( fullSet );
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
    }

    /**
     * Whereas adding {@link Evaluator}s to a {@link TraversalDescription} puts those
     * evaluators in {@code AND-mode} this can group many evaluators in {@code OR-mode}.
     *
     * @param evaluators represented as one evaluators. If any of the evaluators decides
     *                   to include a path it will be included.
     * @return an {@link Evaluator} which decides to include a path if any of the supplied
     *         evaluators wants to include it.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator includeIfAcceptedByAny( final PathEvaluator... evaluators )
    {
        return new PathEvaluator.Adapter()
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
     * @return an {@link Evaluator} which decides to include a path if any of the supplied
     *         evaluators wants to include it.
     */
    @SuppressWarnings( "rawtypes" )
    public static PathEvaluator includeIfAcceptedByAny( final Evaluator... evaluators )
    {
        return new PathEvaluator.Adapter()
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

    public static PathEvaluator endNodeIsAtDepth( final int depth, Node... possibleEndNodes )
    {
        if ( possibleEndNodes.length == 1 )
        {
            final Node target = possibleEndNodes[0];
            return new PathEvaluator.Adapter()
            {
                @Override
                public Evaluation evaluate( Path path, BranchState state )
                {
                    if ( path.length() == depth )
                    {
                        return target.equals( path.endNode() ) ? Evaluation.INCLUDE_AND_PRUNE : Evaluation.EXCLUDE_AND_PRUNE;
                    }
                    else
                    {
                        return Evaluation.EXCLUDE_AND_CONTINUE;
                    }
                }
            };
        }

        final Set<Node> endNodes = new HashSet<Node>( asList( possibleEndNodes ) );
        return new PathEvaluator.Adapter()
        {
            @Override
            public Evaluation evaluate( Path path, BranchState state )
            {
                if ( path.length() == depth )
                {
                    return endNodes.contains( path.endNode() ) ? Evaluation.INCLUDE_AND_PRUNE : Evaluation.EXCLUDE_AND_PRUNE;
                }
                else
                {
                    return Evaluation.EXCLUDE_AND_CONTINUE;
                }
            }
        };
    }
}
