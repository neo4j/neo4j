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

import java.nio.file.Path;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.annotations.documented.Warning;

/**
 * The @Documented annotations are used for error messages in consistency checker.
 */
public interface GBPTreeConsistencyCheckVisitor<KEY>
{
    String indexFile = "Index file: %s.";
    String indexInconsistent = "Index will be excluded from further consistency checks. " + indexFile;

    @Documented( "Index inconsistency: " +
            "Page: %d is not a tree node page. " +
            indexInconsistent )
    void notATreeNode( long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Page: %d has an unknown tree node type: %d.%n" +
            indexInconsistent )
    void unknownTreeNodeType( long pageId, byte treeNodeType, Path file );

    @Documented( "Index inconsistency: " +
            "Sibling pointers misaligned.%n" +
            "Left siblings view:  {%d(%d)}-(%d)->{%d},%n" +
            "Right siblings view: {%d}<-(%d)-{%d(%d)}.%n" +
            indexInconsistent )
    void siblingsDontPointToEachOther(
            long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
            long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration, Path file );

    @Documented( "Index inconsistency: " +
            "Expected rightmost node to have no right sibling but was %d. Current rightmost node is %d.%n" +
            indexInconsistent )
    void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, Path file );

    @Documented( "Index inconsistency: " +
            "We ended up on tree node %d which has a newer generation, successor is: %d.%n" +
            indexInconsistent )
    void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, Path file );

    @Documented( "Index inconsistency: " +
            "Pointer (%s) in tree node %d has pointer generation %d, but target node %d has a higher generation %d.%n" +
            indexInconsistent )
    void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer, long targetNodeGeneration,
            Path file );

    @Documented( "Index inconsistency: " +
            "Keys in tree node %d are out of order.%n" +
            indexInconsistent )
    void keysOutOfOrderInNode( long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Expected range for this tree node is %n%s%n but found %s in position %d, with keyCount %d on page %d.%n" +
            indexInconsistent )
    void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Index has a leaked page that will never be reclaimed, pageId=%d.%n" +
            indexInconsistent )
    void unusedPage( long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Tree node has page id larger than registered last id, lastId=%d, pageId=%d.%n" +
            indexInconsistent )
    void pageIdExceedLastId( long lastId, long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Tree node %d has inconsistent meta data: %s.%n" +
            indexInconsistent )
    void nodeMetaInconsistency( long pageId, String message, Path file );

    @Documented( "Index inconsistency: " +
            "Page id seen multiple times, this means either active tree node is present in freelist or pointers in tree create a loop, pageId=%d.%n" +
            indexInconsistent )
    void pageIdSeenMultipleTimes( long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Crashed pointer found in tree node %d, pointerType='%s',%n" +
            "slotA[generation=%d, readPointer=%d, pointer=%d, state=%s],%n" +
            "slotB[generation=%d, readPointer=%d, pointer=%d, state=%s].%n" +
            indexInconsistent )
    void crashedPointer( long pageId, GBPTreePointerType pointerType,
            long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB, long readPointerB, long pointerB, byte stateB, Path file );

    @Documented( "Index inconsistency: " +
            "Broken pointer found in tree node %d, pointerType='%s',%n" +
            "slotA[generation=%d, readPointer=%d, pointer=%d, state=%s],%n" +
            "slotB[generation=%d, readPointer=%d, pointer=%d, state=%s].%n" +
            indexInconsistent )
    void brokenPointer( long pageId, GBPTreePointerType pointerType,
            long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB, long readPointerB, long pointerB, byte stateB, Path file );

    @Documented( "Index inconsistency: " +
            "Unexpected keyCount on pageId %d, keyCount=%d.%n" +
            indexInconsistent )
    void unreasonableKeyCount( long pageId, int keyCount, Path file );

    @Documented( "Index inconsistency: " +
            "Circular reference, child tree node found among parent nodes. Parents:%n" +
            "%s,%n" +
            "level: %d, pageId: %d.%n" +
            indexInconsistent )
    void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, Path file );

    @Documented( "Index inconsistency: " +
            "Caught exception during consistency check: %s" )
    void exception( Exception e );

    @Warning
    @Documented( "Index was dirty on startup which means it was not shutdown correctly and need to be cleaned up with a successful recovery.%n" +
            indexFile )
    void dirtyOnStartup( Path file );

    class Adaptor<KEY> implements GBPTreeConsistencyCheckVisitor<KEY>
    {
        @Override
        public void notATreeNode( long pageId, Path file )
        {
        }

        @Override
        public void unknownTreeNodeType( long pageId, byte treeNodeType, Path file )
        {
        }

        @Override
        public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
                long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration, Path file )
        {
        }

        @Override
        public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode, Path file )
        {
        }

        @Override
        public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer, Path file )
        {
        }

        @Override
        public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
                long targetNodeGeneration, Path file )
        {
        }

        @Override
        public void keysOutOfOrderInNode( long pageId, Path file )
        {
        }

        @Override
        public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId, Path file )
        {
        }

        @Override
        public void unusedPage( long pageId, Path file )
        {
        }

        @Override
        public void pageIdExceedLastId( long lastId, long pageId, Path file )
        {
        }

        @Override
        public void nodeMetaInconsistency( long pageId, String message, Path file )
        {
        }

        @Override
        public void pageIdSeenMultipleTimes( long pageId, Path file )
        {
        }

        @Override
        public void crashedPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                long generationB, long readPointerB, long pointerB, byte stateB, Path file )
        {
        }

        @Override
        public void brokenPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
                long generationB, long readPointerB, long pointerB, byte stateB, Path file )
        {
        }

        @Override
        public void unreasonableKeyCount( long pageId, int keyCount, Path file )
        {
        }

        @Override
        public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId, Path file )
        {
        }

        @Override
        public void exception( Exception e )
        {
        }

        @Override
        public void dirtyOnStartup( Path file )
        {
        }
    }
}
