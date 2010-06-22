package org.neo4j.kernel;

import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.ExpansionSource;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.PruneEvaluator;
import org.neo4j.graphdb.traversal.SourceSelector;
import org.neo4j.graphdb.traversal.SourceSelectorFactory;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.impl.traversal.FinalExpansionSource;
import org.neo4j.kernel.impl.traversal.TraversalDescriptionImpl;

/**
 * A factory for objects regarding traversal of the graph. F.ex. it has a
 * method {@link #createTraversalDescription()} for creating a new
 * {@link TraversalDescription}, methods for creating new
 * {@link ExpansionSource} instances and more.
 */
public class TraversalFactory
{
    private static final SourceSelectorFactory PREORDER_DEPTH_FIRST_SELECTOR =
            new SourceSelectorFactory()
    {
        public SourceSelector create( ExpansionSource startSource )
        {
            return new PreorderDepthFirstSelector( startSource );
        }
    };
    private static final SourceSelectorFactory POSTORDER_DEPTH_FIRST_SELECTOR =
            new SourceSelectorFactory()
    {
        public SourceSelector create( ExpansionSource startSource )
        {
            return new PostorderDepthFirstSelector( startSource );
        }
    };
    private static final SourceSelectorFactory PREORDER_BREADTH_FIRST_SELECTOR =
            new SourceSelectorFactory()
    {
        public SourceSelector create( ExpansionSource startSource )
        {
            return new PreorderBreadthFirstSelector( startSource );
        }
    };
    private static final SourceSelectorFactory POSTORDER_BREADTH_FIRST_SELECTOR =
            new SourceSelectorFactory()
    {
        public SourceSelector create( ExpansionSource startSource )
        {
            return new PostorderBreadthFirstSelector( startSource );
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
    public static TraversalDescription createTraversalDescription()
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
        return StandardExpander.ALL;
    }

    public static Expander expander( RelationshipExpander expander )
    {
        if ( expander instanceof Expander )
        {
            return (Expander) expander;
        }
        return StandardExpander.wrap( expander );
    }

    /**
     * Combines two {@link ExpansionSource}s with a common
     * {@link ExpansionSource#node() head node} in order to obtain an
     * {@link ExpansionSource} representing a path from the start node of the
     * <code>source</code> {@link ExpansionSource} to the start node of the
     * <code>target</code> {@link ExpansionSource}. The resulting
     * {@link ExpansionSource} will not {@link ExpansionSource#next() expand
     * further}, and does not provide a {@link ExpansionSource#parent() parent}
     * {@link ExpansionSource}.
     *
     * @param source the {@link ExpansionSource} where the resulting path starts
     * @param target the {@link ExpansionSource} where the resulting path ends
     * @throws IllegalArgumentException if the {@link ExpansionSource#node()
     *             head nodes} of the supplied {@link ExpansionSource}s does not
     *             match
     * @return an {@link ExpansionSource} that represents the path from the
     *         start node of the <code>source</code> {@link ExpansionSource} to
     *         the start node of the <code>target</code> {@link ExpansionSource}
     */
    public static ExpansionSource combineSourcePaths( ExpansionSource source,
            ExpansionSource target )
    {
        if ( !source.node().equals( target.node() ) )
        {
            throw new IllegalArgumentException(
                    "The nodes of the head and tail must match" );
        }
        Path headPath = source.position().path(), tailPath = target.position().path();
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
        return new FinalExpansionSource( tailPath.getStartNode(), relationships );
    }

    /**
     * A {@link PruneEvaluator} which prunes everything beyond {@code depth}.
     * @param depth the depth to prune beyond (after).
     * @return a {@link PruneEvaluator} which prunes everything after
     * {@code depth}.
     */
    public static PruneEvaluator pruneAfterDepth( final int depth )
    {
        return new PruneEvaluator()
        {
            public boolean pruneAfter( Position position )
            {
                return position.depth() >= depth;
            }
        };
    }

    /**
     * Returns a "preorder depth first" selector factory . A depth first selector
     * always tries to select positions (from the current position) which are
     * deeper than the current position.
     * @return a {@link SourceSelectorFactory} for a preorder depth first
     * selector.
     */
    public static SourceSelectorFactory preorderDepthFirstSelector()
    {
        return PREORDER_DEPTH_FIRST_SELECTOR;
    }

    /**
     * Returns a "postorder depth first" selector factory. A depth first
     * selector always tries to select positions (from the current position)
     * which are deeper than the current position. A postorder depth first
     * selector selects deeper position before the shallower ones.
     * @return a {@link SourceSelectorFactory} for a postorder depth first
     * selector.
     */
    public static SourceSelectorFactory postorderDepthFirstSelector()
    {
        return POSTORDER_DEPTH_FIRST_SELECTOR;
    }

    /**
     * Returns a "preorder breadth first" selector factory. A breadth first
     * selector always selects all positions on the current depth before
     * advancing to the next depth.
     * @return a {@link SourceSelectorFactory} for a preorder breadth first
     * selector.
     */
    public static SourceSelectorFactory preorderBreadthFirstSelector()
    {
        return PREORDER_BREADTH_FIRST_SELECTOR;
    }

    /**
     * Returns a "postorder breadth first" selector factory. A breadth first
     * selector always selects all positions on the current depth before
     * advancing to the next depth. A postorder breadth first selector selects
     * the levels in the reversed order, starting with the deepest.
     * @return a {@link SourceSelectorFactory} for a postorder breadth first
     * selector.
     */
    public static SourceSelectorFactory postorderBreadthFirstSelector()
    {
        return POSTORDER_BREADTH_FIRST_SELECTOR;
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
            return prefix + "<" + relationship.getType().name() + "," +
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
        Node current = path.getStartNode();
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
}
