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
package org.neo4j.kernel;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.BranchOrderingPolicy;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.TraversalBranch;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.impl.traversal.FinalTraversalBranch;
import org.neo4j.kernel.impl.traversal.TraversalDescriptionImpl;

/**
 * A factory for objects regarding traversal of the graph. F.ex. it has a
 * method {@link #description()} for creating a new
 * {@link TraversalDescription}, methods for creating new
 * {@link TraversalBranch} instances and more.
 */
public class Traversal
{
    private static final Predicate<Path> RETURN_ALL = new Predicate<Path>()
    {
        public boolean accept( Path item )
        {
            return true;
        }
    };
    private static final Predicate<Path> RETURN_ALL_BUT_START_NODE = new Predicate<Path>()
    {
        public boolean accept( Path item )
        {
            return item.length() > 0;
        }
    };

    /**
     * Creates a new {@link TraversalDescription} with default value for
     * everything so that it's OK to call
     * {@link TraversalDescription#traverse(org.neo4j.graphdb.Node)} without
     * modification. But it isn't a very useful traversal, instead you should
     * add rules and behaviours to it before traversing.
     *
     * @return a new {@link TraversalDescription} with default values.
     */
    public static TraversalDescription description()
    {
        return new TraversalDescriptionImpl();
    }

    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with {@code type} and {@code direction}.
     *
     * @param type the {@link RelationshipType} to expand.
     * @param dir the {@link Direction} to expand.
     * @return a new {@link RelationshipExpander}.
     */
    public static Expander expanderForTypes( RelationshipType type,
            Direction dir )
    {
        return StandardExpander.create( type, dir );
    }
    
    /**
     * Creates a new {@link RelationshipExpander} which is set to expand
     * relationships with {@code type} in any direction.
     *
     * @param type the {@link RelationshipType} to expand.
     * @return a new {@link RelationshipExpander}.
     */
    public static Expander expanderForTypes( RelationshipType type )
    {
        return StandardExpander.create( type, Direction.BOTH );
    }

    /**
     * Returns an empty {@link Expander} which, if not modified, will expand
     * all relationships when asked to expand a {@link Node}. Criterias
     * can be added to narrow the {@link Expansion}.
     * @return an empty {@link Expander} which, if not modified, will expand
     * all relationship for {@link Node}s.
     */
    public static Expander emptyExpander()
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
     */
    public static Expander expanderForTypes( RelationshipType type1,
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
     */
    public static Expander expanderForTypes( RelationshipType type1,
            Direction dir1, RelationshipType type2, Direction dir2,
            Object... more )
    {
        return StandardExpander.create( type1, dir1, type2, dir2, more );
    }

    /**
     * Returns a {@link RelationshipExpander} which expands relationships
     * of all types and directions.
     * @return a relationship expander which expands all relationships.
     */
    public static Expander expanderForAllTypes()
    {
        return expanderForAllTypes( Direction.BOTH );
    }

    /**
     * Returns a {@link RelationshipExpander} which expands relationships
     * of all types in the given {@code direction}.
     * @return a relationship expander which expands all relationships in
     * the given {@code direction}.
     */
    public static Expander expanderForAllTypes( Direction direction )
    {
        return StandardExpander.create( direction );
    }

    /**
     * Returns a {@link RelationshipExpander} wrapped as an {@link Expander}.
     * @param expander {@link RelationshipExpander} to wrap.
     * @return a {@link RelationshipExpander} wrapped as an {@link Expander}.
     */
    public static Expander expander( RelationshipExpander expander )
    {
        if ( expander instanceof Expander )
        {
            return (Expander) expander;
        }
        return StandardExpander.wrap( expander );
    }

    /**
     * Combines two {@link TraversalBranch}s with a common
     * {@link TraversalBranch#node() head node} in order to obtain an
     * {@link TraversalBranch} representing a path from the start node of the
     * <code>source</code> {@link TraversalBranch} to the start node of the
     * <code>target</code> {@link TraversalBranch}. The resulting
     * {@link TraversalBranch} will not {@link TraversalBranch#next() expand
     * further}, and does not provide a {@link TraversalBranch#parent() parent}
     * {@link TraversalBranch}.
     *
     * @param source the {@link TraversalBranch} where the resulting path starts
     * @param target the {@link TraversalBranch} where the resulting path ends
     * @throws IllegalArgumentException if the {@link TraversalBranch#node()
     *             head nodes} of the supplied {@link TraversalBranch}s does not
     *             match
     * @return an {@link TraversalBranch} that represents the path from the
     *         start node of the <code>source</code> {@link TraversalBranch} to
     *         the start node of the <code>target</code> {@link TraversalBranch}
     */
    public static TraversalBranch combineSourcePaths( TraversalBranch source,
            TraversalBranch target )
    {
        if ( !source.node().equals( target.node() ) )
        {
            throw new IllegalArgumentException(
                    "The nodes of the head and tail must match" );
        }
        Path headPath = source.position(), tailPath = target.position();
        Relationship[] relationships = new Relationship[headPath.length()
                                                        + tailPath.length()];
        Iterator<Relationship> iter = headPath.relationships().iterator();
        for ( int i = 0; iter.hasNext(); i++ )
        {
            relationships[i] = iter.next();
        }
        iter = tailPath.relationships().iterator();
        for ( int i = relationships.length - 1; iter.hasNext(); i-- )
        {
            relationships[i] = iter.next();
        }
        return new FinalTraversalBranch( tailPath.startNode(), relationships );
    }

    /**
     * A {@link PruneEvaluator} which prunes everything beyond {@code depth}.
     * @param depth the depth to prune beyond (after).
     * @return a {@link PruneEvaluator} which prunes everything after
     * {@code depth}.
     * @deprecated because of the introduction of {@link Evaluator}. The equivalent
     * is {@link Evaluators#toDepth(int)}.
     */
    public static PruneEvaluator pruneAfterDepth( final int depth )
    {
        return new PruneEvaluator()
        {
            public boolean pruneAfter( Path position )
            {
                return position.length() >= depth;
            }
        };
    }

    /**
     * A traversal return filter which returns all {@link Path}s it encounters.
     *
     * @return a return filter which returns everything.
     * @deprecated because of the introduction of {@link Evaluator}. The equivalent
     * is {@link Evaluators#all()}.
     */
    public static Predicate<Path> returnAll()
    {
        return RETURN_ALL;
    }

    /**
     * Returns a filter which accepts items accepted by at least one of the
     * supplied filters.
     *
     * @param filters to group together.
     * @return a {@link Predicate} which accepts if any of the filters accepts.
     */
    public static Predicate<Path> returnAcceptedByAny( final Predicate<Path>... filters )
    {
        return new Predicate<Path>()
        {
            public boolean accept( Path item )
            {
                for ( Predicate<Path> filter : filters )
                {
                    if ( filter.accept( item ) )
                    {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    /**
     * A traversal return filter which returns all {@link Path}s except the
     * position of the start node.
     *
     * @return a return filter which returns everything except the start node.
     * @deprecated because of the introduction of {@link Evaluator}. The equivalent
     * is {@link Evaluators#excludeStartPosition()}.
     */
    public static Predicate<Path> returnAllButStartNode()
    {
        return RETURN_ALL_BUT_START_NODE;
    }

    /**
     * Returns a "preorder depth first" ordering policy. A depth first selector
     * always tries to select positions (from the current position) which are
     * deeper than the current position.
     *
     * @return a {@link BranchOrderingPolicy} for a preorder depth first
     *         selector.
     */
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
     */
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
     */
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
     */
    public static BranchOrderingPolicy postorderBreadthFirst()
    {
        return CommonBranchOrdering.POSTORDER_BREADTH_FIRST;
    }

    /**
     * Provides hooks to help build a string representation of a {@link Path}.
     * @param <T> the type of {@link Path}.
     */
    public static interface PathDescriptor<T extends Path>
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
     */
    public static class DefaultPathDescriptor<T extends Path> implements PathDescriptor<T>
    {
        public String nodeRepresentation( Path path, Node node )
        {
            return "(" + node.getId() + ")";
        }

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
     */
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
     * Returns the default string representation of a {@link Path}. It uses
     * the {@link DefaultPathDescriptor} to get representations.
     * @param path the {@link Path} to build a string representation of.
     * @return the default string representation of a {@link Path}.
     */
    public static String defaultPathToString( Path path )
    {
        return pathToString( path, new DefaultPathDescriptor<Path>() );
    }

    /**
     * Returns a quite simple string representation of a {@link Path}. It
     * doesn't print relationship types or ids, just directions.
     * @param path the {@link Path} to build a string representation of.
     * @return a quite simple representation of a {@link Path}.
     */
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
     */
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

    public static Predicate<Path> returnWhereLastRelationshipTypeIs(
            final RelationshipType firstRelationshipType,
            final RelationshipType... relationshipTypes )
    {
        return new Predicate<Path>()
        {
            public boolean accept( Path p )
            {
                Relationship lastRel = p.lastRelationship();
                if ( lastRel == null )
                {
                    return false;
                }

                if ( lastRel.isType( firstRelationshipType ) )
                {
                    return true;
                }

                for ( RelationshipType currentType : relationshipTypes )
                {
                    if ( lastRel.isType( currentType ) )
                    {
                        return true;
                    }
                }

                return false;
            }
        };
    }
}
