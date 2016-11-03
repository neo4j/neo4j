package org.neo4j.index.bptree;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.bptree.TreeNode.NO_NODE_FLAG;

/**
 * Used to verify a chain of siblings starting with leftmost node.
 * Call {@link #assertNext(PageCursor)} with cursor pointing at sibling expected to be right sibling to previous call
 * to verify that they are indeed linked together correctly.
 * <p>
 * When assertNext has been called on node that is expected to be last in chain, use {@link #assertLast()} to verify.
 */
class RightmostInChain<KEY>
{
    private final TreeNode<KEY,?> node;
    private long currentRightmost;
    private long expectedNextRightmost;

    RightmostInChain( TreeNode<KEY,?> node )
    {
        this.node = node;
        currentRightmost = TreeNode.NO_NODE_FLAG;
        expectedNextRightmost = TreeNode.NO_NODE_FLAG;
    }

    long assertNext( PageCursor cursor )
    {
        long pageId = cursor.getCurrentPageId();
        long leftSibling = node.leftSibling( cursor );
        long rightSibling = node.rightSibling( cursor );

        // Assert we have reached expected node and that we agree about being siblings
        assert leftSibling == currentRightmost :
                "Sibling pointer does align with tree structure. Expected left sibling to be " +
                currentRightmost + " but was " + leftSibling;
        assert pageId == expectedNextRightmost ||
               (expectedNextRightmost == NO_NODE_FLAG && currentRightmost == NO_NODE_FLAG) :
                "Sibling pointer does not align with tree structure. Expected right sibling to be " +
                expectedNextRightmost + " but was " + rightSibling;

        // Update currentRightmost = pageId;
        currentRightmost = pageId;
        expectedNextRightmost = rightSibling;
        return pageId;
    }

    void assertLast()
    {
        assert expectedNextRightmost == NO_NODE_FLAG : "Expected rightmost right sibling to be " +
                                                       NO_NODE_FLAG + " but was " + expectedNextRightmost;
    }
}
