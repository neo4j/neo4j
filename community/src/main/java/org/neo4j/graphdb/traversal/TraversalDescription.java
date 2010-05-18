package org.neo4j.graphdb.traversal;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.graphdb.RelationshipType;

/**
 * Represents a description of a traversal. Here the rules and behaviour of
 * a traversal is described. A traversal description is immutable and each
 * method which adds or modifies the behaviour returns a new instances which
 * includes the new modification, leaving the instance which returns the new
 * instance intact.
 * 
 * When all the rules and behaviours have been described the traversal is
 * started using {@link #traverse(Node)} where a starting node is supplied.
 * The returned {@link Traverser} is then used to step through the graph,
 * returning the positions matching the rules.
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
     * Sets the {@link ReturnFilter} to use, i.e. which positions are OK to
     * return.
     * 
     * @param filter the return filter to use.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription filter( ReturnFilter filter );
    
    /**
     * Sets the {@link SourceSelectorFactory} to use. A {@link SourceSelector}
     * is the basic decisions in the traversal of "where to go next".
     * Examples of default implementations are "breadth first" and
     * "depth first", which can be set with convenience methods
     * {@link #breadthFirst()} and {@link #depthFirst()}.
     * 
     * @param selector the factory which creates the {@link SourceSelector}
     * to use with the traversal.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription sourceSelector( SourceSelectorFactory selector );
    
    /**
     * A convenience method for {@link #sourceSelector(SourceSelectorFactory)}
     * where a "preorder depth first" selector is used. A depth first selector
     * always tries to select positions (from the current position) which are
     * deeper than the current position.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription depthFirst();
    
    /**
     * A convenience method for {@link #sourceSelector(SourceSelectorFactory)}
     * where a "postorder depth first" selector is used. A depth first selector
     * always tries to select positions (from the current position) which are
     * deeper than the current position, where the deeper position are returned
     * before the shallower ones.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription postorderDepthFirst();
    
    /**
     * A convenience method for {@link #sourceSelector(SourceSelectorFactory)}
     * where a "preorder breadth first" selector is used. A breadth first
     * selector always selects all positions on the current depth before
     * advancing to the next depth.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription breadthFirst();
    
    /**
     * A convenience method for {@link #sourceSelector(SourceSelectorFactory)}
     * where a "postorder breadth first" selector is used. A breadth first
     * selector always selects all positions on the current depth before
     * advancing to the next depth. The children are returned before their
     * parent.
     * @return a new traversal description with the new modifications.
     */
    TraversalDescription postorderBreadthFirst();

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
     * behaviour in this description. A {@link Traverser} is returned which
     * is used to step through the graph and getting results back.
     * 
     * @param startNode the {@link Node} to start the traversal from.
     * @return a {@link Traverser} used to step through the graph and to
     * get results from.
     */
    Traverser traverse( Node startNode );
}
