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

import org.neo4j.kernel.impl.annotations.Documented;

public interface GBPTreeConsistencyCheckVisitor<KEY>
{
    @Documented( "Page: %d is not a tree node page." )
    void notATreeNode( long pageId );

    @Documented( "Page: %d has an unknown tree node type: %d." )
    void unknownTreeNodeType( long pageId, byte treeNodeType );

    @Documented( "Sibling pointers misaligned.,%n" +
            "  Left siblings view:  {%d(%d)}-(%d)->{%d},%n" +
            "  Right siblings view: {%d}<-(%d)-{%d(%d)}%n" )
    void siblingsDontPointToEachOther(
            long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
            long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration );

    @Documented( "Expected rightmost node to have no right sibling but was %d. Current rightmost node is %d." )
    void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode );

    @Documented( "We ended up on tree node %d which has a newer generation, successor is: %d" )
    void pointerToOldVersionOfTreeNode( long pageId, long successorPointer );

    @Documented( "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d." )
    void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
            long targetNodeGeneration );

    @Documented( "Keys in tree node %d are out of order." )
    void keysOutOfOrderInNode( long pageId );

    @Documented( "Expected range for this tree node is %n%s%n but found %s in position %d, with keyCount %d on page %d." )
    void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId );

    @Documented( "Index has a leaked page that will never be reclaimed, pageId=%d." )
    void unusedPage( long pageId );

    @Documented( "Tree node has page id larger than registered last id, lastId=%d, pageId=%d." )
    void pageIdExceedLastId( long lastId, long pageId );

    @Documented( "Tree node %d has inconsistent meta data: %s." )
    void nodeMetaInconsistency( long pageId, String message );

    @Documented( "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId=%d." )
    void pageIdSeenMultipleTimes( long pageId );

    @Documented( "Crashed pointer found in tree node %d, pointerType='%s',%n" +
            "  slotA[generation=%d, readPointer=%d, pointer=%d, state=%s],%n" +
            "  slotB[generation=%d, readPointer=%d, pointer=%d, state=%s]" )
    void crashedPointer( long pageId, GBPTreePointerType pointerType,
            long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB, long readPointerB, long pointerB, byte stateB );

    @Documented( "Broken pointer found in tree node %d, pointerType='%s',%n" +
            "  slotA[generation=%d, readPointer=%d, pointer=%d, state=%s],%n" +
            "  slotB[generation=%d, readPointer=%d, pointer=%d, state=%s]" )
    void brokenPointer( long pageId, GBPTreePointerType pointerType,
            long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB, long readPointerB, long pointerB, byte stateB );

    @Documented( "Unexpected keyCount on pageId %d, keyCount=%d" )
    void unreasonableKeyCount( long pageId, int keyCount );

    @Documented( "Circular reference, child tree node found among parent nodes. Parents:%n" +
            "%s,%n" +
            "level: %d, pageId: %d" )
    void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId );

    class Adaptor<KEY> implements GBPTreeConsistencyCheckVisitor<KEY>
    {
        @Override
        public void notATreeNode( long pageId )
        {
        }

        @Override
        public void unknownTreeNodeType( long pageId, byte treeNodeType )
        {
        }

        @Override
        public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
                long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration )
        {
        }

        @Override
        public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode )
        {
        }

        @Override
        public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer )
        {
        }

        @Override
        public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
                long targetNodeGeneration )
        {
        }

        @Override
        public void keysOutOfOrderInNode( long pageId )
        {
        }

        @Override
        public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId )
        {
        }

        @Override
        public void unusedPage( long pageId )
        {
        }

        @Override
        public void pageIdExceedLastId( long lastId, long pageId )
        {
        }

        @Override
        public void nodeMetaInconsistency( long pageId, String message )
        {
        }

        @Override
        public void pageIdSeenMultipleTimes( long pageId )
        {
        }

        @Override
        public void crashedPointer( long pageId, GBPTreePointerType pointerType,
                long generationA, long readPointerA, long pointerA, byte stateA,
                long generationB, long readPointerB, long pointerB, byte stateB )
        {
        }

        @Override
        public void brokenPointer( long pageId, GBPTreePointerType pointerType,
                long generationA, long readPointerA, long pointerA, byte stateA,
                long generationB, long readPointerB, long pointerB, byte stateB )
        {
        }

        @Override
        public void unreasonableKeyCount( long pageId, int keyCount )
        {
        }

        @Override
        public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId )
        {
        }
    }
}
