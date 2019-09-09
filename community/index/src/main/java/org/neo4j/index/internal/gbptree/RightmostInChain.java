/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index.internal.gbptree;

import java.io.File;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

/**
 * Used to verify a chain of siblings starting with leftmost node.
 * Call {@link #assertNext(PageCursor, long, long, long, long, long, GBPTreeConsistencyCheckVisitor)} with cursor pointing at sibling expected
 * to be right sibling to previous call to verify that they are indeed linked together correctly.
 * <p>
 * When assertNext has been called on node that is expected to be last in chain, use {@link #assertLast(GBPTreeConsistencyCheckVisitor)} to verify.
 */
class RightmostInChain
{
    private final File file;
    private long currentRightmostNode = TreeNode.NO_NODE_FLAG;
    private long currentRightmostRightSiblingPointer = TreeNode.NO_NODE_FLAG;
    private long currentRightmostRightSiblingPointerGeneration;
    private long currentRightmostNodeGeneration;

    RightmostInChain( File file )
    {
        this.file = file;
    }

    void assertNext( PageCursor cursor, long newRightmostNodeGeneration,
            long newRightmostLeftSiblingPointer, long newRightmostLeftSiblingPointerGeneration,
            long newRightmostRightSiblingPointer, long newRightmostRightSiblingPointerGeneration,
            GBPTreeConsistencyCheckVisitor visitor )
    {
        long newRightmostNode = cursor.getCurrentPageId();

        // Assert we have reached expected node and that we agree about being siblings
        assertSiblingsAgreeOnBeingSiblings( currentRightmostNode, currentRightmostNodeGeneration, currentRightmostRightSiblingPointer,
                currentRightmostRightSiblingPointerGeneration, newRightmostNode, newRightmostNodeGeneration, newRightmostLeftSiblingPointer,
                newRightmostLeftSiblingPointerGeneration, visitor );
        // Assert that both sibling pointers have reasonable generations
        assertSiblingPointerGeneration( currentRightmostNode, currentRightmostNodeGeneration, currentRightmostRightSiblingPointer,
                currentRightmostRightSiblingPointerGeneration, newRightmostNode, newRightmostNodeGeneration, newRightmostLeftSiblingPointer,
                newRightmostLeftSiblingPointerGeneration, visitor );

        // Update currentRightmostNode = newRightmostNode;
        currentRightmostNode = newRightmostNode;
        currentRightmostNodeGeneration = newRightmostNodeGeneration;
        currentRightmostRightSiblingPointer = newRightmostRightSiblingPointer;
        currentRightmostRightSiblingPointerGeneration = newRightmostRightSiblingPointerGeneration;
    }

    private void assertSiblingPointerGeneration( long currentRightmostNode, long currentRightmostNodeGeneration,
            long currentRightmostRightSiblingPointer, long currentRightmostRightSiblingPointerGeneration, long newRightmostNode,
            long newRightmostNodeGeneration, long newRightmostLeftSiblingPointer, long newRightmostLeftSiblingPointerGeneration,
            GBPTreeConsistencyCheckVisitor visitor )
    {
        if ( currentRightmostNodeGeneration > newRightmostLeftSiblingPointerGeneration && currentRightmostNode != NO_NODE_FLAG )
        {
            // Generation of left sibling is larger than that of the pointer from right sibling
            // Left siblings view:  {_(9)}-(_)->{_}
            // Right siblings view: {_}<-(5)-{_(_)}
            visitor.pointerHasLowerGenerationThanNode( GBPTreePointerType.leftSibling(), newRightmostNode, newRightmostLeftSiblingPointerGeneration,
                    newRightmostLeftSiblingPointer, currentRightmostNodeGeneration, file
            );
        }
        if ( currentRightmostRightSiblingPointerGeneration < newRightmostNodeGeneration &&
                currentRightmostRightSiblingPointer != NO_NODE_FLAG )
        {
            // Generation of right sibling is larger than that of the pointer from left sibling
            // Left siblings view:  {_(_)}-(5)->{_}
            // Right siblings view: {_}<-(_)-{_(9)}
            visitor.pointerHasLowerGenerationThanNode( GBPTreePointerType.rightSibling(), currentRightmostNode, currentRightmostRightSiblingPointerGeneration,
                    currentRightmostRightSiblingPointer, newRightmostNodeGeneration, file
            );
        }
    }

    private void assertSiblingsAgreeOnBeingSiblings( long currentRightmostNode, long currentRightmostNodeGeneration,
            long currentRightmostRightSiblingPointer, long currentRightmostRightSiblingPointerGeneration, long newRightmostNode,
            long newRightmostNodeGeneration, long newRightmostLeftSiblingPointer, long newRightmostLeftSiblingPointerGeneration,
            GBPTreeConsistencyCheckVisitor visitor )
    {
        boolean siblingsPointToEachOther = true;
        if ( newRightmostLeftSiblingPointer != currentRightmostNode )
        {
            // Right sibling does not point to left sibling
            // Left siblings view:  {2(_)}-(_)->{_}
            // Right siblings view: {1}<-(_)-{_(_)}
            siblingsPointToEachOther = false;
        }
        if ( newRightmostNode != currentRightmostRightSiblingPointer &&
                (currentRightmostRightSiblingPointer != NO_NODE_FLAG || currentRightmostNode != NO_NODE_FLAG) )
        {
            // Left sibling does not point to right sibling
            // Left siblings view:  {_(_)}-(_)->{1}
            // Right siblings view: {_}<-(_)-{2(_)}
            siblingsPointToEachOther = false;
        }
        if ( !siblingsPointToEachOther )
        {
            visitor.siblingsDontPointToEachOther( currentRightmostNode, currentRightmostNodeGeneration, currentRightmostRightSiblingPointerGeneration,
                    currentRightmostRightSiblingPointer, newRightmostLeftSiblingPointer, newRightmostLeftSiblingPointerGeneration, newRightmostNode,
                    newRightmostNodeGeneration, file
            );
        }
    }

    void assertLast( GBPTreeConsistencyCheckVisitor visitor )
    {
        if ( currentRightmostRightSiblingPointer != NO_NODE_FLAG )
        {
            visitor.rightmostNodeHasRightSibling( currentRightmostRightSiblingPointer, currentRightmostNode, file );
        }
    }
}
