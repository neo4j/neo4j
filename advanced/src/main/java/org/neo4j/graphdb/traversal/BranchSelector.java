package org.neo4j.graphdb.traversal;

/**
 * Decides "where to go next" in a traversal. It keeps state itself, f.ex.
 * its own current position. Examples of implementations are "depth first"
 * and "breadth first". This is an interface to implement if you'd like to
 * implement f.ex. a "best first" selector based on your own criterias.
 */
public interface BranchSelector
{
    /**
     * Decides the next position ("where to go from here") from the current
     * position, based on the {@code rules}. Since {@link TraversalBranch}
     * has the {@link TraversalBranch#node()} of the position and the
     * {@link TraversalBranch#relationship()} to how it got there as well as
     * {@link TraversalBranch#position()}, decisions
     * can be based on the current expansion source and the given rules.
     * 
     * @return the next position based on the current position and the
     * {@code rules} of the traversal.
     */
    TraversalBranch next();
}
