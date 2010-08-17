package org.neo4j.graphdb.traversal;

public interface UniquenessFilter
{
    /**
     * The check whether or not to expand the first branch is a separate
     * method because it may contain checks which would be unnecessary for
     * all other checks. So it's purely an optimization.
     * 
     * @param branch the first branch to check, i.e. the branch representing
     * the start node in the traversal.
     * @return whether or not {@code branch} is unique, and hence can be
     * visited in this traversal.
     */
    boolean checkFirst( TraversalBranch branch );
    
    /**
     * Checks whether or not {@code branch} is unique, and hence can be
     * visited in this traversal.
     * @param branch the {@link TraversalBranch} to check for uniqueness.
     * @param remember whether or not to remember {@code branch}. If
     * {@code remember} is {@code false} then no state in this filter must be
     * changed.
     * @return whether or not {@code branch} is unique, and hence can be
     * visited in this traversal.
     */
    boolean check( TraversalBranch branch, boolean remember );
}
