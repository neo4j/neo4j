package org.neo4j.graphdb.traversal;

import java.util.Iterator;

/**
 * Represents the "rules" of the traversal. The traversal can ask this class
 * questions like: "should I traverse beyound this point?" or
 * "should I return this position?".
 * 
 * It applies the rules and restrictions specified by a
 * {@link TraversalDescription}.
 */
public interface TraversalRules extends Iterator<Position>
{
    /**
     * Decides whether or not the traversal should visit the {@code source}
     * and proceed traversing from it. The decision is based on
     * {@link Uniqueness}.
     * 
     * @param source the {@link ExpansionSource} to decide whether or not to
     * proceed with or not.
     * @return whether or not {@code source} should be visited and then,
     * furthermore, traversed from.
     */
    boolean okToProceed( ExpansionSource source );
    
    /**
     * Decides whether or not the traversal should expand {@code source}
     * (get its relationships) so that it can traverse beyond it. The decision
     * is based on pruning (see {@link PruneEvaluator}) and {@link Uniqueness}.
     * @param source
     * @return
     */
    boolean shouldExpandBeyond( ExpansionSource source );

    /**
     * Decides whether or not the {@code source} should be returned. When a
     * source is decided to be returned it will show up as the next item in
     * a {@link Traverser}.
     * 
     * @param source the {@link ExpansionSource} to decide on whether or not
     * to return.
     * @return whether or not to return the position represented by
     * {@code source}.
     */
    boolean okToReturn( ExpansionSource source );
}
