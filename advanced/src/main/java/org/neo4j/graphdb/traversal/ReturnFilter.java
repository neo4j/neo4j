package org.neo4j.graphdb.traversal;

/**
 * Represents a filter which can decide whether or not to return a given
 * position or not.
 */
public interface ReturnFilter
{
    /**
     * A completely hollow filter which lets all positions pass.
     */
    static final ReturnFilter ALL = new ReturnFilter()
    {
        public boolean shouldReturn( Position position )
        {
            return true;
        }
    };
    
    /**
     * Lets all positions pass, except the start node.
     */
    static final ReturnFilter ALL_BUT_START_NODE = new ReturnFilter()
    {
        public boolean shouldReturn( Position position )
        {
            return !position.atStartNode();
        }
    };

    /**
     * Decides whether or not the ongoing {@link Traverser} should include
     * the given {@code position} in the result, i.e. return it as a result.
     * 
     * @param position the {@link Position} to decide whether to return or not.
     * @return whether or not the ongoing {@link Traverser} should return
     * the {@code position}.
     */
    boolean shouldReturn( Position position );
}
