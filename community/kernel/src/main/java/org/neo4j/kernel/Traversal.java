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
package org.neo4j.kernel;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.BranchCollisionDetector;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.InitialStateFactory;
import org.neo4j.graphdb.traversal.SideSelectorPolicy;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.UniquenessFactory;
import org.neo4j.kernel.impl.traversal.BidirectionalTraversalDescriptionImpl;
import org.neo4j.kernel.impl.traversal.MonoDirectionalTraversalDescription;

/**
 * A factory for objects regarding traversal of the graph. F.ex. it has a
 * method {@link #traversal()} for creating a new
 * {@link TraversalDescription}, methods for creating new
 * {@link TraversalBranch} instances and more.
 *
 * @deprecated See {@link org.neo4j.graphdb.GraphDatabaseService#traversalDescription} and
 * {@link org.neo4j.graphdb.GraphDatabaseService#bidirectionalTraversalDescription} plus
 * {@link org.neo4j.graphdb.PathExpanders}, {@link org.neo4j.graphdb.traversal.SideSelectorPolicies},
 * {@link org.neo4j.graphdb.traversal.BranchOrderingPolicies},
 * {@link org.neo4j.graphdb.traversal.BranchCollisionPolicies},
 * {@link org.neo4j.graphdb.traversal.Paths} and {@link org.neo4j.graphdb.traversal.Uniqueness}
 */
@Deprecated
public class Traversal
{
    /**
     * Creates a new {@link TraversalDescription} with default value for
     * everything so that it's OK to call
     * {@link TraversalDescription#traverse(org.neo4j.graphdb.Node)} without
     * modification. But it isn't a very useful traversal, instead you should
     * add rules and behaviors to it before traversing.
     *
     * @return a new {@link TraversalDescription} with default values.
     * @deprecated See {@link org.neo4j.graphdb.GraphDatabaseService#traversalDescription}
     */
    @Deprecated
    public static TraversalDescription description()
    {
        return new MonoDirectionalTraversalDescription();
    }

    /**
     * More convenient name than {@link #description()} when using static imports.
     * Does the same thing.
     *
     * @deprecated See {@link org.neo4j.graphdb.GraphDatabaseService#traversalDescription}
     */
    @Deprecated
    public static TraversalDescription traversal()
    {
        return new MonoDirectionalTraversalDescription();
    }

    /**
     * @deprecated See {@link org.neo4j.graphdb.GraphDatabaseService#traversalDescription}
     */
    @Deprecated
    public static TraversalDescription traversal( UniquenessFactory uniqueness )
    {
        return new MonoDirectionalTraversalDescription().uniqueness( uniqueness );
    }

    /**
     * @deprecated See {@link org.neo4j.graphdb.GraphDatabaseService#traversalDescription}
     */
    @Deprecated
    public static TraversalDescription traversal( UniquenessFactory uniqueness, Object optionalUniquenessParameter )
    {
        return new MonoDirectionalTraversalDescription().uniqueness( uniqueness, optionalUniquenessParameter );
    }

    /**
     * @deprecated See {@link org.neo4j.graphdb.GraphDatabaseService#bidirectionalTraversalDescription}
     */
    @Deprecated
    public static BidirectionalTraversalDescription bidirectionalTraversal()
    {
        return new BidirectionalTraversalDescriptionImpl();
    }

    /**
     * {@link InitialStateFactory} which always returns the supplied {@code initialState}.
     * @param initialState the initial state for a traversal branch.
     * @return an {@link InitialStateFactory} which always will return the supplied
     * {@code initialState}.
     *
     * @deprecated because InitialStateFactory is deprecated.
     */
    @Deprecated
    public static <STATE> InitialStateFactory<STATE> initialState( final STATE initialState )
    {
        return new InitialStateFactory<STATE>()
        {
            @Override
            public STATE initialState( Path branch )
            {
                return initialState;
            }
        };
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with {@code type} and {@code direction}.
     *
     * @param type the {@link RelationshipType} to expand.
     * @param dir the {@link Direction} to expand.
     * @return a new {@link RelationshipExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forTypeAndDirection}
     */
    @Deprecated
    public static Expander expanderForTypes( RelationshipType type,
            Direction dir )
    {
        return StandardExpander.create( type, dir );
    }

    /**
     * Creates a new {@link PathExpander} which is set to expand
     * relationships with {@code type} and {@code direction}.
     *
     * @param type the {@link RelationshipType} to expand.
     * @param dir the {@link Direction} to expand.
     * @return a new {@link PathExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forTypeAndDirection}
     */
    @Deprecated
    @SuppressWarnings( "unchecked" )
    public static <STATE> PathExpander<STATE> pathExpanderForTypes( RelationshipType type, Direction dir )
    {
        return StandardExpander.create( type, dir );
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with {@code type} in any direction.
     *
     * @param type the {@link RelationshipType} to expand.
     * @return a new {@link RelationshipExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forType}
     */
    @Deprecated
    public static Expander expanderForTypes( RelationshipType type )
    {
        return StandardExpander.create( type, Direction.BOTH );
    }

    /**
     * Creates a new {@link PathExpander} which is set to expand
     * relationships with {@code type} in any direction.
     *
     * @param type the {@link RelationshipType} to expand.
     * @return a new {@link PathExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forType}
     */
    @Deprecated
    @SuppressWarnings( "unchecked" )
    public static <STATE> PathExpander<STATE> pathExpanderForTypes( RelationshipType type )
    {
        return StandardExpander.create( type, Direction.BOTH );
    }

    /**
     * Returns an empty {@link Expander} which, if not modified, will expand
     * all relationships when asked to expand a {@link Node}. Criteria
     * can be added to narrow the {@link Expansion}.
     * @return an empty {@link Expander} which, if not modified, will expand
     * all relationship for {@link Node}s.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#allTypesAndDirections}
     */
    @Deprecated
    public static Expander emptyExpander()
    {
        return StandardExpander.DEFAULT; // TODO: should this be a PROPER empty?
    }

    /**
     * Returns an empty {@link PathExpander} which, if not modified, will expand
     * all relationships when asked to expand a {@link Node}. Criteria
     * can be added to narrow the {@link Expansion}.
     * @return an empty {@link PathExpander} which, if not modified, will expand
     * all relationship for {@link Path}s.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#allTypesAndDirections}
     */
    @Deprecated
    @SuppressWarnings( "unchecked" )
    public static <STATE> PathExpander<STATE> emptyPathExpander()
    {
        return StandardExpander.DEFAULT; // TODO: should this be a PROPER empty?
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with two different types and directions.
     *
     * @param type1 a {@link RelationshipType} to expand.
     * @param dir1 a {@link Direction} to expand.
     * @param type2 another {@link RelationshipType} to expand.
     * @param dir2 another {@link Direction} to expand.
     * @return a new {@link RelationshipExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forTypesAndDirections}
     */
    @Deprecated
    public static Expander expanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2 )
    {
        return StandardExpander.create( type1, dir1, type2, dir2 );
    }

    /**
     * Creates a new {@link PathExpander} which is set to expand
     * relationships with two different types and directions.
     *
     * @param type1 a {@link RelationshipType} to expand.
     * @param dir1 a {@link Direction} to expand.
     * @param type2 another {@link RelationshipType} to expand.
     * @param dir2 another {@link Direction} to expand.
     * @return a new {@link PathExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forTypesAndDirections}
     */
    @Deprecated
    @SuppressWarnings( "unchecked" )
    public static <STATE> PathExpander<STATE> pathExpanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2 )
    {
        return StandardExpander.create( type1, dir1, type2, dir2 );
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with multiple types and directions.
     *
     * @param type1 a {@link RelationshipType} to expand.
     * @param dir1 a {@link Direction} to expand.
     * @param type2 another {@link RelationshipType} to expand.
     * @param dir2 another {@link Direction} to expand.
     * @param more additional pairs or type/direction to expand.
     * @return a new {@link RelationshipExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forTypesAndDirections}
     */
    @Deprecated
    public static Expander expanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        return StandardExpander.create( type1, dir1, type2, dir2, more );
    }

    /**
     * Creates a new {@link PathExpander} which is set to expand
     * relationships with multiple types and directions.
     *
     * @param type1 a {@link RelationshipType} to expand.
     * @param dir1 a {@link Direction} to expand.
     * @param type2 another {@link RelationshipType} to expand.
     * @param dir2 another {@link Direction} to expand.
     * @param more additional pairs or type/direction to expand.
     * @return a new {@link PathExpander}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forTypesAndDirections}
     */
    @Deprecated
    @SuppressWarnings( "unchecked" )
    public static <STATE> PathExpander<STATE> pathExpanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        return StandardExpander.create( type1, dir1, type2, dir2, more );
    }

    /**
     * Returns a {@link RelationshipExpander} which expands relationships
     * of all types and directions.
     * @return a relationship expander which expands all relationships.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#allTypesAndDirections}
     */
    @Deprecated
    public static Expander expanderForAllTypes()
    {
        return expanderForAllTypes( Direction.BOTH );
    }

    /**
     * Returns a {@link RelationshipExpander} which expands relationships
     * of all types and directions.
     * @return a relationship expander which expands all relationships.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#allTypesAndDirections}
     */
    @Deprecated
    public static <STATE> PathExpander<STATE> pathExpanderForAllTypes()
    {
        return PathExpanders.allTypesAndDirections();
    }

    /**
     * Returns a {@link RelationshipExpander} which expands relationships
     * of all types in the given {@code direction}.
     * @return a relationship expander which expands all relationships in
     * the given {@code direction}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forDirection}
     */
    @Deprecated
    public static Expander expanderForAllTypes( Direction direction )
    {
        return StandardExpander.create( direction );
    }

    /**
     * Returns a {@link PathExpander} which expands relationships
     * of all types in the given {@code direction}.
     * @return a path expander which expands all relationships in
     * the given {@code direction}.
     *
     * @deprecated See {@link org.neo4j.graphdb.PathExpanders#forDirection}
     */
    @Deprecated
    @SuppressWarnings( "unchecked" )
    public static <STATE> PathExpander<STATE> pathExpanderForAllTypes( Direction direction )
    {
        return StandardExpander.create( direction );
    }

    /**
     * @deprecated Because {@link RelationshipExpander} is deprecated. Use {@link PathExpander} instead.
     */
    @Deprecated
    public static Expander expander( PathExpander expander )
    {
        if ( expander instanceof Expander )
        {
            return (Expander) expander;
        }
        return StandardExpander.wrap( expander );
    }

    /**
     * Returns a {@link RelationshipExpander} wrapped as an {@link Expander}.
     * @param expander {@link RelationshipExpander} to wrap.
     * @return a {@link RelationshipExpander} wrapped as an {@link Expander}.
     * @deprecated Because {@link RelationshipExpander} is deprecated. Use {@link PathExpander} instead.
     */
    @Deprecated
    public static Expander expander( RelationshipExpander expander )
    {
        if ( expander instanceof Expander )
        {
            return (Expander) expander;
        }
        return StandardExpander.wrap( expander );
    }

    /**
     * Returns a "preorder depth first" ordering policy. A depth first selector
     * always tries to select positions (from the current position) which are
     * deeper than the current position.
     *
     * @return a {@link BranchOrderingPolicy} for a preorder depth first
     *         selector.
     *
     * @deprecated See {@link org.neo4j.graphdb.traversal.BranchOrderingPolicies#PREORDER_DEPTH_FIRST}
     */
    @Deprecated
    public static BranchOrderingPolicy preorderDepthFirst()
    {
        return CommonBranchOrdering.PREORDER_DEPTH_FIRST;
    }

    /**
     * Returns a "postorder depth first" ordering policy. A depth first selector
     * always tries to select positions (from the current position) which are
     * deeper than the current position. A postorder depth first selector
     * selects deeper position before the shallower ones.
     *
     * @return a {@link BranchOrderingPolicy} for a postorder depth first
     *         selector.
     *
     * @deprecated See {@link org.neo4j.graphdb.traversal.BranchOrderingPolicies#POSTORDER_DEPTH_FIRST}
     */
    @Deprecated
    public static BranchOrderingPolicy postorderDepthFirst()
    {
        return CommonBranchOrdering.POSTORDER_DEPTH_FIRST;
    }

    /**
     * Returns a "preorder breadth first" ordering policy. A breadth first
     * selector always selects all positions on the current depth before
     * advancing to the next depth.
     *
     * @return a {@link BranchOrderingPolicy} for a preorder breadth first
     *         selector.
     *
     * @deprecated See {@link org.neo4j.graphdb.traversal.BranchOrderingPolicies#PREORDER_BREADTH_FIRST}
     */
    @Deprecated
    public static BranchOrderingPolicy preorderBreadthFirst()
    {
        return CommonBranchOrdering.PREORDER_BREADTH_FIRST;
    }

    /**
     * Returns a "postorder breadth first" ordering policy. A breadth first
     * selector always selects all positions on the current depth before
     * advancing to the next depth. A postorder breadth first selector selects
     * the levels in the reversed order, starting with the deepest.
     *
     * @return a {@link BranchOrderingPolicy} for a postorder breadth first
     *         selector.
     *
     * @deprecated See {@link org.neo4j.graphdb.traversal.BranchOrderingPolicies#POSTORDER_BREADTH_FIRST}
     */
    @Deprecated
    public static BranchOrderingPolicy postorderBreadthFirst()
    {
        return CommonBranchOrdering.POSTORDER_BREADTH_FIRST;
    }

    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.SideSelectorPolicies#ALTERNATING}
     */
    @Deprecated
    public static SideSelectorPolicy alternatingSelectorOrdering()
    {
        return SideSelectorPolicies.ALTERNATING;
    }

    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.SideSelectorPolicies#LEVEL}
     */
    @Deprecated
    public static SideSelectorPolicy levelSelectorOrdering()
    {
        return SideSelectorPolicies.LEVEL;
    }

    /**
     * @deprecated See {@link org.neo4j.graphdb.traversal.BranchCollisionPolicies#SHORTEST_PATH}
     */
    @Deprecated
    public static BranchCollisionDetector shortestPathsCollisionDetector( int maxDepth )
    {
        return new ShortestPathsBranchCollisionDetector( Evaluators.toDepth( maxDepth ),
                                                         Predicates.<Path>alwaysTrue() );
    }

    /**
     * Provides hooks to help build a string representation of a {@link Path}.
     * @param <T> the type of {@link Path}.
     * @deprecated Use {@link org.neo4j.graphdb.traversal.Paths.PathDescriptor} instead
     */
    @Deprecated
    public interface PathDescriptor<T extends Path>
    {
        /**
         * Returns a string representation of a {@link Node}.
         * @param path the {@link Path} we're building a string representation
         * from.
         * @param node the {@link Node} to return a string representation of.
         * @return a string representation of a {@link Node}.
         */
        String nodeRepresentation( T path, Node node );

        /**
         * Returns a string representation of a {@link Relationship}.
         * @param path the {@link Path} we're building a string representation
         * from.
         * @param from the previous {@link Node} in the path.
         * @param relationship the {@link Relationship} to return a string
         * representation of.
         * @return a string representation of a {@link Relationship}.
         */
        String relationshipRepresentation( T path, Node from,
                Relationship relationship );
    }

    /**
     * The default {@link PathDescriptor} used in common toString()
     * representations in classes implementing {@link Path}.
     * @param <T> the type of {@link Path}.
     * @deprecated Use {@link org.neo4j.graphdb.traversal.Paths.DefaultPathDescriptor} instead.
     */
    @Deprecated
    public static class DefaultPathDescriptor<T extends Path> implements PathDescriptor<T>
    {
        @Override
        public String nodeRepresentation( Path path, Node node )
        {
            return "(" + node.getId() + ")";
        }

        @Override
        public String relationshipRepresentation( Path path,
                Node from, Relationship relationship )
        {
            String prefix = "--", suffix = "--";
            if ( from.equals( relationship.getEndNode() ) )
            {
                prefix = "<--";
            }
            else
            {
                suffix = "-->";
            }
            return prefix + "[" + relationship.getType().name() + "," +
                    relationship.getId() + "]" + suffix;
        }
    }

    /**
     * Method for building a string representation of a {@link Path}, using
     * the given {@code builder}.
     * @param <T> the type of {@link Path}.
     * @param path the {@link Path} to build a string representation of.
     * @param builder the {@link PathDescriptor} to get
     * {@link Node} and {@link Relationship} representations from.
     * @return a string representation of a {@link Path}.
     * @deprecated Use {@link org.neo4j.graphdb.traversal.Paths#pathToString(org.neo4j.graphdb.Path,
     *             org.neo4j.graphdb.traversal.Paths.PathDescriptor)} instead.
     */
    @Deprecated
    public static <T extends Path> String pathToString( T path, PathDescriptor<T> builder )
    {
        Node current = path.startNode();
        StringBuilder result = new StringBuilder();
        for ( Relationship rel : path.relationships() )
        {
            result.append( builder.nodeRepresentation( path, current ) );
            result.append( builder.relationshipRepresentation( path, current, rel ) );
            current = rel.getOtherNode( current );
        }
        result.append( builder.nodeRepresentation( path, current ) );
        return result.toString();
    }

    /**
     * TODO: This method re-binds nodes and relationships. It should not.
     *
     * Returns the default string representation of a {@link Path}. It uses
     * the {@link DefaultPathDescriptor} to get representations.
     * @param path the {@link Path} to build a string representation of.
     * @return the default string representation of a {@link Path}.
     * @deprecated Use {@link org.neo4j.graphdb.traversal.Paths#defaultPathToString(org.neo4j.graphdb.Path)} instead.
     */
    @Deprecated
    public static String defaultPathToString( Path path )
    {
        return pathToString( path, new DefaultPathDescriptor<Path>() );
    }

    /**
     * Returns a quite simple string representation of a {@link Path}. It
     * doesn't print relationship types or ids, just directions.
     * @param path the {@link Path} to build a string representation of.
     * @return a quite simple representation of a {@link Path}.
     * @deprecated Use {@link org.neo4j.graphdb.traversal.Paths#simplePathToString(org.neo4j.graphdb.Path)} instead.
     */
    @Deprecated
    public static String simplePathToString( Path path )
    {
        return pathToString( path, new DefaultPathDescriptor<Path>()
        {
            @Override
            public String relationshipRepresentation( Path path, Node from,
                    Relationship relationship )
            {
                return relationship.getStartNode().equals( from ) ? "-->" : "<--";
            }
        } );
    }

    /**
     * Returns a quite simple string representation of a {@link Path}. It
     * doesn't print relationship types or ids, just directions. it uses the
     * {@code nodePropertyKey} to try to display that property value as in the
     * node representation instead of the node id. If that property doesn't
     * exist, the id is used.
     * @param path the {@link Path} to build a string representation of.
     * @return a quite simple representation of a {@link Path}.
     * @deprecated Use {@link org.neo4j.graphdb.traversal.Paths#simplePathToString(org.neo4j.graphdb.Path, String)}
     *             instead.
     */
    @Deprecated
    public static String simplePathToString( Path path, final String nodePropertyKey )
    {
        return pathToString( path, new DefaultPathDescriptor<Path>()
        {
            @Override
            public String nodeRepresentation( Path path, Node node )
            {
                return "(" + node.getProperty( nodePropertyKey, node.getId() ) + ")";
            }

            @Override
            public String relationshipRepresentation( Path path, Node from,
                    Relationship relationship )
            {
                return relationship.getStartNode().equals( from ) ? "-->" : "<--";
            }
        } );
    }

    /**
     * @deprecated This was an experimental feature which we have rolled back.
     */
    @Deprecated
    public static PathDescription path()
    {
        return new PathDescription();
    }
}
