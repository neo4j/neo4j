package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.helpers.Predicate;

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
     * Sets the {@link Uniqueness} to use.
     *
     * @param uniqueness the {@link Uniqueness} to use.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription uniqueness( Uniqueness uniqueness );

    /**
     * Sets the {@link Uniqueness} to use. It also accepts an extra parameter
     * which is obligatory for certain uniqueness's, f.ex
     * {@link Uniqueness#NODE_RECENT}.
     *
     * @param uniqueness the {@link Uniqueness} to use.
     * @param parameter the extra parameter to go with the uniqueness.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription uniqueness( Uniqueness uniqueness, Object parameter );

    /**
     * Adds {@code pruning} to the list of {@link PruneEvaluator}s which
     * are used to prune the traversal. The semantics for many prune evaluators
     * is that if any one of the added prune evaluators returns {@code true}
     * it's considered OK to prune there.
     *
     * @param pruning the {@link PruneEvaluator} to add to the list of prune
     * evaluators to use.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription prune( PruneEvaluator pruning );

    /**
     * Sets the return filter to use, that is which positions are OK to return.
     * Each position is represented by a {@link Path} from the start node of the
     * traversal to the current node. The current node is the
     * {@link Path#endNode()} of the path.
     *
     * @param filter the {@link Predicate} to use as filter.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription filter( Predicate<Path> filter );

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
     * Sets the {@link RelationshipExpander} as the expander of relationships,
     * discarding all previous calls to
     * {@link #relationships(RelationshipType)} and
     * {@link #relationships(RelationshipType, Direction)}.
     *
     * @param expander the {@link RelationshipExpander} to use.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription expand( RelationshipExpander expander );

    /**
     * Starts traversing from {@code startNode} based on all the rules and
     * behavior in this description. A {@link Traverser} is returned which is
     * used to step through the graph and getting results back.
     *
     * @param startNode the {@link Node} to start the traversal from.
     * @return a {@link Traverser} used to step through the graph and to get
     *         results from.
     */
    Traverser traverse( Node startNode );
}
