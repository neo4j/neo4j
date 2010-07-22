package org.neo4j.graphdb.traversal;

/**
 * Creator of {@link BranchSelector} instances with a starting point to base
 * the first decision on.
 */
public interface BranchOrderingPolicy
{
    /**
     * Instantiates a {@link BranchSelector} with {@code startBranch} as the
     * first branch to base a decision on "where to go next".
     *
     * @param startBranch the {@link TraversalBranch} to start from.
     * @return a new {@link BranchSelector} used to decide "where to go next" in
     *         the traversal.
     */
    BranchSelector create( TraversalBranch startBranch );
}
