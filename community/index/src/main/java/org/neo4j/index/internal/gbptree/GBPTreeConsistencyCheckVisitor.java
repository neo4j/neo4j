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

import org.neo4j.kernel.impl.annotations.Documented;

/**
 * The @Documented annotations are used for error messages in consistency checker.
 */
public interface GBPTreeConsistencyCheckVisitor<KEY>
{
    String indexFile = "Index file: %s.";

    @Documented( "Index inconsistency: " +
            "Page: %d is not a tree node page. " +
            indexFile )
    void notATreeNode( long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Page: %d has an unknown tree node type: %d.%n" +
            indexFile )
    void unknownTreeNodeType( long pageId, byte treeNodeType, File file );

    @Documented( "Index inconsistency: " +
            "Sibling pointers misaligned.%n" +
            "Left siblings view:  {%d(%d)}-(%d)->{%d},%n" +
            "Right siblings view: {%d}<-(%d)-{%d(%d)}.%n" +
            indexFile )
    void siblingsDontPointToEachOther(
            long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
            long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration, File file );

    @Documented( "Index inconsistency: " +
            "Expected rightmost node to have no right sibling but was %d. Current rightmost node is %d.%n" +
            indexFile )
    void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, File file );

    @Documented( "Index inconsistency: " +
            "We ended up on tree node %d which has a newer generation, successor is: %d.%n" +
            indexFile )
    void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, File file );

    @Documented( "Index inconsistency: " +
            "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d.%n" +
            indexFile )
    void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer, long targetNodeGeneration,
            File file );

    @Documented( "Index inconsistency: " +
            "Keys in tree node %d are out of order.%n" +
            indexFile )
    void keysOutOfOrderInNode( long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Expected range for this tree node is %n%s%n but found %s in position %d, with keyCount %d on page %d.%n" +
            indexFile )
    void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Index has a leaked page that will never be reclaimed, pageId=%d.%n" +
            indexFile )
    void unusedPage( long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Tree node has page id larger than registered last id, lastId=%d, pageId=%d.%n" +
            indexFile )
    void pageIdExceedLastId( long lastId, long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Tree node %d has inconsistent meta data: %s.%n" +
            indexFile )
    void nodeMetaInconsistency( long pageId, String message, File file );

    @Documented( "Index inconsistency: " +
            "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId=%d.%n" +
            indexFile )
    void pageIdSeenMultipleTimes( long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Crashed pointer found in tree node %d, pointerType='%s',%n" +
            "slotA[generation=%d, readPointer=%d, pointer=%d, state=%s],%n" +
            "slotB[generation=%d, readPointer=%d, pointer=%d, state=%s].%n" +
            indexFile )
    void crashedPointer( long pageId, GBPTreePointerType pointerType,
            long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB, long readPointerB, long pointerB, byte stateB, File file );

    @Documented( "Index inconsistency: " +
            "Broken pointer found in tree node %d, pointerType='%s',%n" +
            "slotA[generation=%d, readPointer=%d, pointer=%d, state=%s],%n" +
            "slotB[generation=%d, readPointer=%d, pointer=%d, state=%s].%n" +
            indexFile )
    void brokenPointer( long pageId, GBPTreePointerType pointerType,
            long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB, long readPointerB, long pointerB, byte stateB, File file );

    @Documented( "Index inconsistency: " +
            "Unexpected keyCount on pageId %d, keyCount=%d.%n" +
            indexFile )
    void unreasonableKeyCount( long pageId, int keyCount, File file );

    @Documented( "Index inconsistency: " +
            "Circular reference, child tree node found among parent nodes. Parents:%n" +
            "%s,%n" +
            "level: %d, pageId: %d.%n" +
            indexFile )
    void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, File file );

    @Documented( "Index inconsistency: " +
            "Caught exception during consistency check: %s" )
    void exception( Exception e );

    class Adaptor<KEY> implements GBPTreeConsistencyCheckVisitor<KEY>
    {
        @Override
        public void notATreeNode( long pageId, File file )
        {
        }

        @Override
        public void unknownTreeNodeType( long pageId, byte treeNodeType, File file )
        {
        }

        @Override
        public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
                long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration, File file )
        {
        }

        @Override
        public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, File file )
        {
        }

        @Override
        public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, File file )
        {
        }

        @Override
        public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
                long targetNodeGeneration, File file )
        {
        }

        @Override
        public void keysOutOfOrderInNode( long pageId, File file )
        {
        }

        @Override
        public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, File file )
        {
        }

        @Override
        public void unusedPage( long pageId, File file )
        {
        }

        @Override
        public void pageIdExceedLastId( long lastId, long pageId, File file )
        {
        }

        @Override
        public void nodeMetaInconsistency( long pageId, String message, File file )
        {
        }

        @Override
        public void pageIdSeenMultipleTimes( long pageId, File file )
        {
        }

        @Override
        public void crashedPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                long generationB, long readPointerB, long pointerB, byte stateB, File file )
        {
        }

        @Override
        public void brokenPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                long generationB, long readPointerB, long pointerB, byte stateB, File file )
        {
        }

        @Override
        public void unreasonableKeyCount( long pageId, int keyCount, File file )
        {
        }

        @Override
        public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, File file )
        {
        }

        @Override
        public void exception( Exception e )
        {
        }
    }
}
