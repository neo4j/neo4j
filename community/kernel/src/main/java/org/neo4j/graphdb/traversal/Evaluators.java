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
package org.neo4j.graphdb.traversal;

import static java.util.Arrays.asList;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

/**
 * Common {@link Evaluator}s useful during common traversals.
 * 
 * @author Mattias Persson
 * @see Evaluator
 * @see TraversalDescription
 */
public abstract class Evaluators
{
    private static final Evaluator ALL = new Evaluator()
    {
        public Evaluation evaluate( Path path )
        {
            return Evaluation.INCLUDE_AND_CONTINUE;
        }
    };
    
    private static final Evaluator ALL_BUT_START_POSITION = new Evaluator()
    {
        public Evaluation evaluate( Path path )
        {
            return path.length() == 0 ? Evaluation.EXCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_CONTINUE;
        }
    };
    
    /**
     * @return an evaluator which includes everything it encounters and doesn't prune
     * anything.
     */
    public static Evaluator all()
    {
        return ALL;
    }
    
    /**
     * @return an evaluator which never prunes and includes everything except
     * the first position, i.e. the the start node.
     */
    public static Evaluator excludeStartPosition()
    {
        return ALL_BUT_START_POSITION;
    }
    
    /**
     * Returns an {@link Evaluator} which includes positions down to {@code depth}
     * and prunes everything deeper than that.
     * 
     * @param depth the max depth to traverse to.
     * @return Returns an {@link Evaluator} which includes positions down to
     * {@code depth} and prunes everything deeper than that.
     */
    public static Evaluator toDepth( final int depth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return path.length() < depth ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.INCLUDE_AND_PRUNE;
            }
        };
    }
    
    /**
     * Returns an {@link Evaluator} which only includes positions from {@code depth}
     * and deeper and never prunes anything.
     * 
     * @param depth the depth to start include positions from.
     * @return Returns an {@link Evaluator} which only includes positions from
     * {@code depth} and deeper and never prunes anything.
     */
    public static Evaluator fromDepth( final int depth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return path.length() >= depth ? Evaluation.INCLUDE_AND_CONTINUE : Evaluation.EXCLUDE_AND_CONTINUE;
            }
        };
    }
    
    /**
     * Returns an {@link Evaluator} which only includes positions at {@code depth}
     * and prunes everything deeper than that.
     * 
     * @param depth the depth to start include positions from.
     * @return Returns an {@link Evaluator} which only includes positions at
     * {@code depth} and prunes everything deeper than that.
     */
    public static Evaluator atDepth( final int depth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
            {
                return path.length() == depth ? Evaluation.INCLUDE_AND_PRUNE: Evaluation.EXCLUDE_AND_CONTINUE;
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
     * depths {@code minDepth} and {@code maxDepth}. It prunes everything deeper
     * than {@code maxDepth}.
     */
    public static Evaluator includingDepths( final int minDepth, final int maxDepth )
    {
        return new Evaluator()
        {
            public Evaluation evaluate( Path path )
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
     * @param evaluationIfMatch the {@link Evaluation} to return if the type of the
     * last relationship in the path matches any of the given types.
     * @param evaluationIfNoMatch the {@link Evaluation} to return if the type of the
     * last relationship in the path doesn't match any of the given types.
     * @param type the (first) type (of possibly many) to match the last relationship
     * in paths with.
     * @param orAnyOfTheseTypes additional types to match the last relationship in
     * paths with.
     * @return an {@link Evaluator} which compares the type of the last relationship
     * in a {@link Path} to a given set of relationship types.
     */
    public static Evaluator lastRelationshipTypeIs( final Evaluation evaluationIfMatch,
            final Evaluation evaluationIfNoMatch, RelationshipType type,
            RelationshipType... orAnyOfTheseTypes )
    {
        final Set<String> expectedTypes = new HashSet<String>();
        expectedTypes.add( type.name() );
        for ( RelationshipType otherType : orAnyOfTheseTypes )
        {
            expectedTypes.add( otherType.name() );
        }
        
        return new Evaluator()
        {
            @Override
            public Evaluation evaluate( Path path )
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
     * @see #lastRelationshipTypeIs(Evaluation, Evaluation, RelationshipType, RelationshipType...).
     * Uses {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfMatch}
     * and {@link Evaluation#EXCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     * 
     * @param type the (first) type (of possibly many) to match the last relationship
     * in paths with.
     * @param orAnyOfTheseTypes additional types to match the last relationship in
     * paths with.
     * @return an {@link Evaluator} which compares the type of the last relationship
     * in a {@link Path} to a given set of relationship types.
     */
    public static Evaluator returnWhereLastRelationshipTypeIs( RelationshipType type,
            RelationshipType... orAnyOfTheseTypes )
    {
        return lastRelationshipTypeIs( Evaluation.INCLUDE_AND_CONTINUE, Evaluation.EXCLUDE_AND_CONTINUE,
                type, orAnyOfTheseTypes );
    }
    
    /**
     * @see #lastRelationshipTypeIs(Evaluation, Evaluation, RelationshipType, RelationshipType...).
     * Uses {@link Evaluation#INCLUDE_AND_PRUNE} for {@code evaluationIfMatch}
     * and {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     * 
     * @param type the (first) type (of possibly many) to match the last relationship
     * in paths with.
     * @param orAnyOfTheseTypes additional types to match the last relationship in
     * paths with.
     * @return an {@link Evaluator} which compares the type of the last relationship
     * in a {@link Path} to a given set of relationship types.
     */
    public static Evaluator pruneWhereLastRelationshipTypeIs( RelationshipType type,
            RelationshipType... orAnyOfTheseTypes )
    {
        return lastRelationshipTypeIs( Evaluation.INCLUDE_AND_PRUNE, Evaluation.EXCLUDE_AND_CONTINUE,
                type, orAnyOfTheseTypes );
    }
    
    /**
     * Returns an {@link Evaluator} which looks at each {@link Path} and includes those
     * where the {@link Path#endNode()} is one of {@code possibleEndNodes}.
     * 
     * @param evaluationIfMatch the {@link Evaluation} to return for those paths which
     * have a matching end node.
     * @param evaluationIfNoMatch  the {@link Evaluation} to return for those paths which
     * doesn't have a matching end node.
     * @param possibleEndNodes end nodes for paths to be included in the result.
     * @return an {@link Evaluator} which looks at each {@link Path} and includes those
     * where the {@link Path#endNode()} is one of {@code possibleEndNodes}.
     */
    public static Evaluator endNodeIs( final Evaluation evaluationIfMatch,
            final Evaluation evaluationIfNoMatch, Node... possibleEndNodes )
    {
        if ( possibleEndNodes.length == 1 )
        {
            final Node target = possibleEndNodes[0];
            return new Evaluator()
            {
                @Override
                public Evaluation evaluate( Path path )
                {
                    return target.equals( path.endNode() ) ? evaluationIfMatch : evaluationIfNoMatch;
                }
            };
        }
        
        final Set<Node> endNodes = new HashSet<Node>( asList( possibleEndNodes ) );
        return new Evaluator()
        {
            @Override
            public Evaluation evaluate( Path path )
            {
                return endNodes.contains( path.endNode() ) ? evaluationIfMatch : evaluationIfNoMatch;
            }
        };
    }
    
    /**
     * @see #endNodeIs(Evaluation, Evaluation, Node...),
     * uses {@link Evaluation#INCLUDE_AND_CONTINUE} for {@code evaluationIfMatch}
     * and {@link Evaluation#EXCLUDE_AND_CONTINUE} for {@code evaluationIfNoMatch}.
     * @param possibleEndNodes end nodes for paths to be included in the result.
     */
    public static Evaluator returnWhereEndNodeIs( Node... possibleEndNodes )
    {
        return endNodeIs( Evaluation.INCLUDE_AND_CONTINUE, Evaluation.EXCLUDE_AND_CONTINUE, possibleEndNodes );
    }
}
