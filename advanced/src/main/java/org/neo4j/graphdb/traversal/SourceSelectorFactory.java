package org.neo4j.graphdb.traversal;

/**
 * Creator of {@link SourceSelector} instances with a starting point to base
 * the first decision on.
 */
public interface SourceSelectorFactory
{
    /**
     * Instantiates a {@link SourceSelector} with {@code startSource} as the
     * first source to base a decision on "where to go next".
     * 
     * @param startSource the {@link ExpansionSource} to start from.
     * @return a new {@link SourceSelector} used to decide "where to go next"
     * in the traversal.
     */
    SourceSelector create( ExpansionSource startSource );
}
