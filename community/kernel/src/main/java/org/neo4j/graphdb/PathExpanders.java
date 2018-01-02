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
package org.neo4j.graphdb;

import java.io.PrintStream;

import org.neo4j.function.BiFunction;
import org.neo4j.graphdb.traversal.BranchState;
import org.neo4j.graphdb.traversal.Paths;
import org.neo4j.kernel.StandardExpander;

/**
 * A catalog of convenient {@link PathExpander} factory methods.
 * <p>
 * Use {@link PathExpanderBuilder} to build specialized {@link PathExpander}s.
 */
// Copied from kernel package so that we can hide kernel from the public API.
public abstract class PathExpanders
{
    /**
     * A very permissive {@link PathExpander} that follows any type in any direction.
     * 
     * @param <STATE> the type of the object that holds the state
     * @return a very permissive {@link PathExpander} that follows any type in any direction
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> allTypesAndDirections()
    {
        return StandardExpander.DEFAULT;
    }

    /**
     * A very permissive {@link PathExpander} that follows {@code type} relationships in any direction.
     * 
     * @param type the type of relationships to expand in any direction
     * @param <STATE> the type of the object that holds the state
     * @return a very permissive {@link PathExpander} that follows {@code type} relationships in any direction
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forType( RelationshipType type )
    {
        return StandardExpander.create( type, Direction.BOTH );
    }

    /**
     * A very permissive {@link PathExpander} that follows any type in {@code direction}.
     * 
     * @param direction the direction to follow relationships in
     * @param <STATE> the type of the object that holds the state
     * @return a very permissive {@link PathExpander} that follows any type in {@code direction}
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forDirection( Direction direction )
    {
        return StandardExpander.create( direction );
    }

    /**
     * A very restricted {@link PathExpander} that follows {@code type} in {@code direction}.
     * 
     * @param type the type of relationships to follow
     * @param direction the direction to follow relationships in
     * @param <STATE> the type of the object that holds the state
     * @return a very restricted {@link PathExpander} that follows {@code type} in {@code direction}
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forTypeAndDirection( RelationshipType type, Direction direction )
    {
        return StandardExpander.create( type, direction );
    }

    /**
     * A very restricted {@link PathExpander} that follows only the {@code type}/{@code direction} pairs that you list.
     * 
     * @param type1 the type of relationships to follow in {@code direction1}
     * @param direction1 the direction to follow {@code type1} relationships in
     * @param type2 the type of relationships to follow in {@code direction2}
     * @param direction2 the direction to follow {@code type2} relationships in
     * @param more add more {@code type}/{@code direction} pairs
     * @param <STATE> the type of the object that holds the state
     * @return a very restricted {@link PathExpander} that follows only the {@code type}/{@code direction} pairs that you list
     */
    @SuppressWarnings("unchecked")
    public static <STATE> PathExpander<STATE> forTypesAndDirections( RelationshipType type1, Direction direction1,
                                                                     RelationshipType type2, Direction direction2,
                                                                     Object... more )
    {
        return StandardExpander.create( type1, direction1, type2, direction2, more );
    }

    /**
     * An expander forcing constant relationship direction
     * 
     * @param types types of relationships to follow
     * @param <STATE> the type of the object that holds the state
     * @return a {@link PathExpander} which enforces constant relationship direction
     */
    public static <STATE> PathExpander<STATE> forConstantDirectionWithTypes( final RelationshipType... types )
    {
        return new PathExpander<STATE>()
        {
            @Override
            public Iterable<Relationship> expand( Path path, BranchState<STATE> state )
            {
                if ( path.length() == 0 )
                {
                    return path.endNode().getRelationships( types );
                }
                else
                {
                    Direction direction = getDirectionOfLastRelationship( path );
                    return path.endNode().getRelationships( direction, types );
                }
            }

            @Override
            public PathExpander<STATE> reverse()
            {
                return this;
            }

            private Direction getDirectionOfLastRelationship( Path path )
            {
                assert path.length() > 0;
                Direction direction = Direction.INCOMING;
                if ( path.endNode().equals( path.lastRelationship().getEndNode() ) )
                {
                    direction = Direction.OUTGOING;
                }
                return direction;
            }
        };
    }

    private PathExpanders()
    {
        // you should never instantiate this
    }

    /**
     * A wrapper that uses {@link Paths.DefaultPathDescriptor} to print expanded paths.
     * All expanded paths will be printed using System.out.
     * @param source    {@link PathExpander} to wrap.
     * @param <STATE>   the type of the object that holds the state
     * @return A new {@link PathExpander}.
     */
    public static <STATE> PathExpander<STATE> printingWrapper( final PathExpander<STATE> source )
    {
        return printingWrapper( source, new Paths.DefaultPathDescriptor() );
    }


    /**
     * A wrapper that uses {@link Paths.DefaultPathDescriptor} to print expanded paths that fulfill
     * {@link BiFunction} predicate.
     * Will use System.out as {@link PrintStream}.
     * @param source    {@link PathExpander} to wrap.
     * @param pred      {@link BiFunction} used as predicate for printing expansion.
     * @param <STATE>   the type of the object that holds the state
     * @return          A new {@link PathExpander}.
     */
    public static <STATE> PathExpander<STATE> printingWrapper(
            final PathExpander<STATE> source,
            final BiFunction<Path, BranchState, Boolean> pred )
    {
        return printingWrapper( source, pred, new Paths.DefaultPathDescriptor() );
    }

    /**
     * A wrapper that uses {@link Paths.DefaultPathDescriptor} to print expanded paths using given
     * {@link Paths.PathDescriptor}.
     * All expanded paths will be printed.
     * Will use System.out as {@link PrintStream}.
     * @param source        {@link PathExpander} to wrap.
     * @param descriptor    {@link Paths.PathDescriptor} to use when printing paths.
     * @param <STATE>       the type of the object that holds the state
     * @return              A new {@link PathExpander}.
     */
    public static <STATE> PathExpander<STATE> printingWrapper(
            final PathExpander<STATE> source,
            final Paths.PathDescriptor descriptor )
    {
        return printingWrapper( source, new BiFunction<Path,BranchState,Boolean>()
        {
            @Override
            public Boolean apply( Path propertyContainers, BranchState stateBranchState )
            {
                return true;
            }
        }, descriptor );
    }

    /**
     * A wrapper that uses {@link Paths.DefaultPathDescriptor} to print expanded paths that fulfill
     * {@link BiFunction} predicate using given {@link Paths.PathDescriptor}.
     * Will use System.out as {@link PrintStream}.
     * @param source    {@link PathExpander} to wrap.
     * @param pred      {@link BiFunction} used as predicate for printing expansion.
     * @param descriptor    {@link Paths.PathDescriptor} to use when printing paths.
     * @param <STATE>   the type of the object that holds the state
     * @return          A new {@link PathExpander}.
     */
    public static <STATE> PathExpander<STATE> printingWrapper(
            final PathExpander<STATE> source,
            final BiFunction<Path, BranchState, Boolean> pred,
            final Paths.PathDescriptor descriptor )
    {
        return printingWrapper( source, pred, descriptor, System.out );
    }

    /**
     * A wrapper that uses {@link Paths.DefaultPathDescriptor} to print expanded paths that fulfill
     * {@link BiFunction} predicate using given {@link Paths.PathDescriptor}.
     * @param source        {@link PathExpander} to wrap.
     * @param pred          {@link BiFunction} used as predicate for printing expansion.
     * @param descriptor    {@link Paths.PathDescriptor} to use when printing paths.
     * @param out           {@link PrintStream} to use for printing expanded paths
     * @param <STATE>       the type of the object that holds the state
     * @return              A new {@link PathExpander}.
     */
    public static <STATE> PathExpander<STATE> printingWrapper(
            final PathExpander<STATE> source,
            final BiFunction<Path, BranchState, Boolean> pred,
            final Paths.PathDescriptor descriptor,
            final PrintStream out )
    {
        return new PathExpander<STATE>()
        {
            @Override
            public Iterable<Relationship> expand( Path path, BranchState state )
            {
                if ( pred.apply( path, state ) )
                {
                    out.println( Paths.pathToString( path, descriptor ) );
                }
                return source.expand( path, state );
            }

            @Override
            public PathExpander<STATE> reverse()
            {
                return printingWrapper( source.reverse(), pred, descriptor, out );
            }
        };
    }
}
