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

import org.neo4j.io.pagecache.PageCursor;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

/**
 * Used to verify a chain of siblings starting with leftmost node.
 * Call {@link #assertNext(PageCursor, long, long, long, long, long)} with cursor pointing at sibling expected
 * to be right sibling to previous call to verify that they are indeed linked together correctly.
 * <p>
 * When assertNext has been called on node that is expected to be last in chain, use {@link #assertLast()} to verify.
 */
class RightmostInChain
{
    private long currentRightmostNode = TreeNode.NO_NODE_FLAG;
    private long currentRightmostRightSiblingPointer = TreeNode.NO_NODE_FLAG;
    private long currentRightmostRightSiblingPointerGeneration;
    private long currentRightmostNodeGeneration;

    void assertNext( PageCursor cursor, long newRightmostNodeGeneration,
            long newRightmostLeftSiblingPointer, long newRightmostLeftSiblingPointerGeneration,
            long newRightmostRightSiblingPointer, long newRightmostRightSiblingPointerGeneration )
    {
        long newRightmostNode = cursor.getCurrentPageId();

        // Assert we have reached expected node and that we agree about being siblings
        StringBuilder errorMessageBuilder = new StringBuilder();
        if ( newRightmostLeftSiblingPointer != currentRightmostNode )
        {
            errorMessageBuilder.append( format( "Sibling pointer does align with tree structure%n" ) );
        }
        if ( currentRightmostNodeGeneration > newRightmostLeftSiblingPointerGeneration && currentRightmostNode != NO_NODE_FLAG )
        {
            errorMessageBuilder.append( format( "Sibling pointer generation differs from expected%n" ) );
        }
        if ( newRightmostNode != currentRightmostRightSiblingPointer &&
                (currentRightmostRightSiblingPointer != NO_NODE_FLAG || currentRightmostNode != NO_NODE_FLAG) )
        {
            errorMessageBuilder.append( format( "Sibling pointer does not align with tree structure%n" ) );
        }
        if ( currentRightmostRightSiblingPointerGeneration < newRightmostNodeGeneration &&
                currentRightmostRightSiblingPointer != NO_NODE_FLAG )
        {
            errorMessageBuilder.append( format( "Sibling pointer generation differs from expected%n" ) );
        }

        String errorMessage = errorMessageBuilder.toString();
        if ( !errorMessage.equals( "" ) )
        {
            errorMessage = addPatternToExceptionMessage( newRightmostNodeGeneration, newRightmostLeftSiblingPointer,
                    newRightmostLeftSiblingPointerGeneration, newRightmostNode, errorMessage );
            throw new TreeInconsistencyException( errorMessage );
        }

        // Update currentRightmostNode = newRightmostNode;
        currentRightmostNode = newRightmostNode;
        currentRightmostNodeGeneration = newRightmostNodeGeneration;
        currentRightmostRightSiblingPointer = newRightmostRightSiblingPointer;
        currentRightmostRightSiblingPointerGeneration = newRightmostRightSiblingPointerGeneration;
    }

    private String addPatternToExceptionMessage( long newRightmostGeneration, long leftSibling,
            long leftSiblingGeneration, long newRightmost, String errorMessage )
    {
        return format( "%s" +
                        "  Left siblings view:  %s%n" +
                        "  Right siblings view: %s%n", errorMessage,
                leftPattern( currentRightmostNode, currentRightmostNodeGeneration,
                        currentRightmostRightSiblingPointerGeneration,
                        currentRightmostRightSiblingPointer ),
                rightPattern( newRightmost, newRightmostGeneration, leftSiblingGeneration, leftSibling ) );
    }

    private String leftPattern( long actualLeftSibling, long actualLeftSiblingGeneration,
            long expectedRightSiblingGeneration, long expectedRightSibling )
    {
        return format( "{%d(%d)}-(%d)->{%d}", actualLeftSibling, actualLeftSiblingGeneration, expectedRightSiblingGeneration,
                expectedRightSibling );
    }

    private String rightPattern( long actualRightSibling, long actualRightSiblingGeneration,
            long expectedLeftSiblingGeneration, long expectedLeftSibling )
    {
        return format( "{%d}<-(%d)-{%d(%d)}", expectedLeftSibling, expectedLeftSiblingGeneration, actualRightSibling,
                actualRightSiblingGeneration );
    }

    void assertLast()
    {
        assert currentRightmostRightSiblingPointer == NO_NODE_FLAG : "Expected rightmost right sibling to be " + NO_NODE_FLAG
                + " but was " + currentRightmostRightSiblingPointer;
    }
}
