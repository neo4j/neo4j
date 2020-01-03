/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.helpers.Exceptions;

import static java.lang.String.format;
import static org.neo4j.index.internal.gbptree.TreeNode.NO_NODE_FLAG;

public class ThrowingConsistencyCheckVisitor<KEY> implements GBPTreeConsistencyCheckVisitor<KEY>
{
    private static final String treeStructureInconsistency = "Tree structure inconsistency: ";
    private static final String keyOrderInconsistency = "Key order inconsistency: ";
    private static final String nodeMetaInconsistency = "Node meta inconsistency: ";
    private static final String treeMetaInconsistency = "Tree meta inconsistency: ";
    private static final String unexpectedExceptionInconsistency = "Unexpected exception inconsistency: ";

    @Override
    public void notATreeNode( long pageId, File file )
    {
        throwTreeStructureInconsistency( "Page: %d is not a tree node page.", pageId );
    }

    @Override
    public void unknownTreeNodeType( long pageId, byte treeNodeType, File file )
    {
        throwTreeStructureInconsistency( "Page: %d has an unknown tree node type: %d.", pageId, treeNodeType );
    }

    @Override
    public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
            long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration, File file )
    {
        throwTreeStructureInconsistency( "Sibling pointers misaligned.%n" +
                        "  Left siblings view:  %s%n" +
                        "  Right siblings view: %s%n",
                leftPattern( leftNode, leftNodeGeneration, leftRightSiblingPointerGeneration, leftRightSiblingPointer ),
                rightPattern( rightNode, rightNodeGeneration, rightLeftSiblingPointerGeneration, rightLeftSiblingPointer ) );
    }

    @Override
    public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, File file )
    {
        throwTreeStructureInconsistency( "Expected rightmost right sibling to be %d but was %d. Current rightmost node is %d.",
                NO_NODE_FLAG, rightSiblingPointer, rightmostNode );
    }

    @Override
    public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, File file )
    {
        throwTreeStructureInconsistency( "We ended up on tree node %d which has a newer generation, successor is: %d", pageId, successorPointer );
    }

    @Override
    public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
            long targetNodeGeneration, File file )
    {
        throwTreeStructureInconsistency( "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d.",
                pointerType.toString(), sourceNode, pointerGeneration, pointer, targetNodeGeneration );
    }

    @Override
    public void keysOutOfOrderInNode( long pageId, File file )
    {
        throwKeyOrderInconsistency( "Keys in tree node %d are out of order.", pageId );
    }

    @Override
    public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, File file )
    {
        throwKeyOrderInconsistency( "Expected range for this tree node is %n%s%n but found %s in position %d, with keyCount %d on page %d.",
                range, key, pos, keyCount, pageId );
    }

    @Override
    public void unusedPage( long pageId, File file )
    {
        throwTreeMetaInconsistency( "Index has a leaked page that will never be reclaimed, pageId=%d.", pageId );
    }

    @Override
    public void pageIdExceedLastId( long lastId, long pageId, File file )
    {
        throwTreeMetaInconsistency( "Tree node has page id larger than registered last id, lastId=%d, pageId=%d.", lastId, pageId );
    }

    @Override
    public void nodeMetaInconsistency( long pageId, String message, File file )
    {
        throwNodeMetaInconsistency( "Tree node %d has inconsistent meta data: %s.", pageId, message );
    }

    @Override
    public void pageIdSeenMultipleTimes( long pageId, File file )
    {
        throwTreeStructureInconsistency(
                "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId=%d.",
                pageId );
    }

    @Override
    public void crashedPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA, long generationB,
            long readPointerB, long pointerB, byte stateB, File file )
    {
        throwTreeStructureInconsistency( "Crashed pointer found in tree node %d, pointer: %s%n  slotA[%s]%n  slotB[%s]",
                pageId, pointerType.toString(),
                stateToString( generationA, readPointerA, pointerA, stateA ),
                stateToString( generationB, readPointerB, pointerB, stateB ) );
    }

    @Override
    public void brokenPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA, long generationB,
            long readPointerB, long pointerB, byte stateB, File file )
    {
        throwTreeStructureInconsistency( "Broken pointer found in tree node %d, pointer: %s%n  slotA[%s]%n  slotB[%s]",
                pageId, pointerType.toString(),
                stateToString( generationA, readPointerA, pointerA, stateA ),
                stateToString( generationB, readPointerB, pointerB, stateB ) );
    }

    @Override
    public void unreasonableKeyCount( long pageId, int keyCount, File file )
    {
        throwTreeMetaInconsistency( "Unexpected keyCount on pageId %d, keyCount=%d", pageId, keyCount );
    }

    @Override
    public void childNodeFoundAmongParentNodes( KeyRange<KEY> parentRange, int level, long pageId, File file )
    {
        throwTreeStructureInconsistency( "Circular reference, child tree node found among parent nodes. Parents:%n%s%nlevel: %d, pageId: %d",
                parentRange, level, pageId );
    }

    @Override
    public void exception( Exception e )
    {
        throwUnexpectedExceptionInconsistency( "%s", Exceptions.stringify( e ) );
    }

    private static String stateToString( long generation, long readPointer, long pointer, byte stateA )
    {
        return format( "generation=%d, readPointer=%d, pointer=%d, state=%s",
                generation, readPointer, pointer, GenerationSafePointerPair.pointerStateName( stateA ) );
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

    private void throwKeyOrderInconsistency( String format, Object... args )
    {
        throwWithPrefix( keyOrderInconsistency, format, args );
    }

    private void throwTreeStructureInconsistency( String format, Object... args )
    {
        throwWithPrefix( treeStructureInconsistency, format, args );
    }

    private void throwNodeMetaInconsistency( String format, Object... args )
    {
        throwWithPrefix( nodeMetaInconsistency, format, args );
    }

    private void throwTreeMetaInconsistency( String format, Object... args )
    {
        throwWithPrefix( treeMetaInconsistency, format, args );
    }

    private void throwUnexpectedExceptionInconsistency( String format, Object... args )
    {
        throwWithPrefix( unexpectedExceptionInconsistency, format, args );
    }

    private void throwWithPrefix( String prefix, String format, Object[] args )
    {
        throw new TreeInconsistencyException( String.format( prefix + format, args ) );
    }
}
