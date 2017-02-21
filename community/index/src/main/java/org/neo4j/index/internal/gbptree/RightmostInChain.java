/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
    private long currentRightmostRightSiblingPointerGen;
    private long currentRightmostNodeGen;

    void assertNext( PageCursor cursor, long newRightmostNodeGen,
            long newRightmostLeftSiblingPointer, long newRightmostLeftSiblingPointerGen,
            long newRightmostRightSiblingPointer, long newRightmostRightSiblingPointerGen )
    {
        long newRightmostNode = cursor.getCurrentPageId();

        // Assert we have reached expected node and that we agree about being siblings
        StringBuilder errorMessageBuilder = new StringBuilder();
        if ( newRightmostLeftSiblingPointer != currentRightmostNode )
        {
            errorMessageBuilder.append( format( "Sibling pointer does align with tree structure%n" ) );
        }
        if ( currentRightmostNodeGen > newRightmostLeftSiblingPointerGen && currentRightmostNode != NO_NODE_FLAG )
        {
            errorMessageBuilder.append( format( "Sibling pointer gen differs from expected%n" ) );
        }
        if ( newRightmostNode != currentRightmostRightSiblingPointer &&
                (currentRightmostRightSiblingPointer != NO_NODE_FLAG || currentRightmostNode != NO_NODE_FLAG) )
        {
            errorMessageBuilder.append( format( "Sibling pointer does not align with tree structure%n" ) );
        }
        if ( currentRightmostRightSiblingPointerGen < newRightmostNodeGen &&
                currentRightmostRightSiblingPointer != NO_NODE_FLAG )
        {
            errorMessageBuilder.append( format( "Sibling pointer gen differs from expected%n" ) );
        }

        String errorMessage = errorMessageBuilder.toString();
        if ( !errorMessage.equals( "" ) )
        {
            setPatternException( cursor, newRightmostNodeGen, newRightmostLeftSiblingPointer, newRightmostLeftSiblingPointerGen, newRightmostNode, errorMessage );
        }

        // Update currentRightmostNode = newRightmostNode;
        currentRightmostNode = newRightmostNode;
        currentRightmostNodeGen = newRightmostNodeGen;
        currentRightmostRightSiblingPointer = newRightmostRightSiblingPointer;
        currentRightmostRightSiblingPointerGen = newRightmostRightSiblingPointerGen;
    }

    private void setPatternException( PageCursor cursor, long newRightmostGen, long leftSibling,
            long leftSiblingGen, long newRightmost, String errorMessage )
    {
        cursor.setCursorException( format( "%s" +
                        "  Left siblings view:  %s%n" +
                        "  Right siblings view: %s%n", errorMessage,
                leftPattern( currentRightmostNode, currentRightmostNodeGen, currentRightmostRightSiblingPointerGen,
                        currentRightmostRightSiblingPointer ),
                rightPattern( newRightmost, newRightmostGen, leftSiblingGen, leftSibling ) ) );
    }

    private String leftPattern( long actualLeftSibling, long actualLeftSiblingGen, long expectedRightSiblingGen,
            long expectedRightSibling )
    {
        return format( "{%d(%d)}-(%d)->{%d}", actualLeftSibling, actualLeftSiblingGen, expectedRightSiblingGen,
                expectedRightSibling );
    }

    private String rightPattern( long actualRightSibling, long actualRightSiblingGen, long expectedLeftSiblingGen,
            long expectedLeftSibling )
    {
        return format( "{%d}<-(%d)-{%d(%d)}", expectedLeftSibling, expectedLeftSiblingGen, actualRightSibling,
                actualRightSiblingGen );
    }

    void assertLast()
    {
        assert currentRightmostRightSiblingPointer == NO_NODE_FLAG : "Expected rightmost right sibling to be " + NO_NODE_FLAG
                + " but was " + currentRightmostRightSiblingPointer;
    }
}
