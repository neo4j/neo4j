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

import org.apache.commons.lang3.mutable.MutableBoolean;

import org.neo4j.kernel.impl.annotations.Documented;

class CleanTrackingConsistencyCheckVisitor<KEY> implements GBPTreeConsistencyCheckVisitor<KEY>
{
    private final MutableBoolean clean = new MutableBoolean( true );
    private final GBPTreeConsistencyCheckVisitor<KEY> delegate;

    CleanTrackingConsistencyCheckVisitor( GBPTreeConsistencyCheckVisitor<KEY> delegate )
    {
        this.delegate = delegate;
    }

    boolean clean()
    {
        return clean.booleanValue();
    }

    @Override
    @Documented( "notATreeNode" )
    public void notATreeNode( long pageId )
    {
        clean.setFalse();
        delegate.notATreeNode( pageId );
    }

    @Override
    @Documented( "unknownTreeNodeType" )
    public void unknownTreeNodeType( long pageId, byte treeNodeType )
    {
        clean.setFalse();
        delegate.unknownTreeNodeType( pageId, treeNodeType );
    }

    @Override
    @Documented( "siblingsDontPointToEachOther" )
    public void siblingsDontPointToEachOther( long leftNode, long leftNodeGeneration, long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
            long rightLeftSiblingPointer, long rightLeftSiblingPointerGeneration, long rightNode, long rightNodeGeneration )
    {
        clean.setFalse();
        delegate.siblingsDontPointToEachOther( leftNode, leftNodeGeneration, leftRightSiblingPointerGeneration, leftRightSiblingPointer,
                rightLeftSiblingPointer, rightLeftSiblingPointerGeneration, rightNode, rightNodeGeneration
        );
    }

    @Override
    @Documented( "rightmostNodeHasRightSibling" )
    public void rightmostNodeHasRightSibling( long rightSiblingPointer, long rightmostNode )
    {
        clean.setFalse();
        delegate.rightmostNodeHasRightSibling( rightSiblingPointer, rightmostNode );
    }

    @Override
    @Documented( "pointerToOldVersionOfTreeNode" )
    public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer )
    {
        clean.setFalse();
        delegate.pointerToOldVersionOfTreeNode( pageId, successorPointer );
    }

    @Override
    @Documented( "pointerHasLowerGenerationThanNode" )
    public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointerGeneration, long pointer,
            long targetNodeGeneration )
    {
        clean.setFalse();
        delegate.pointerHasLowerGenerationThanNode( pointerType, sourceNode, pointerGeneration, pointer, targetNodeGeneration );
    }

    @Override
    @Documented( "keysOutOfOrderInNode" )
    public void keysOutOfOrderInNode( long pageId )
    {
        clean.setFalse();
        delegate.keysOutOfOrderInNode( pageId );
    }

    @Override
    @Documented( "keysLocatedInWrongNode" )
    public void keysLocatedInWrongNode( KeyRange<KEY> range, KEY key, int pos, int keyCount, long pageId )
    {
        clean.setFalse();
        delegate.keysLocatedInWrongNode( range, key, pos, keyCount, pageId );
    }

    @Override
    @Documented( "unusedPage" )
    public void unusedPage( long pageId )
    {
        clean.setFalse();
        delegate.unusedPage( pageId );
    }

    @Override
    @Documented( "pageIdExceedLastId" )
    public void pageIdExceedLastId( long lastId, long pageId )
    {
        clean.setFalse();
        delegate.pageIdExceedLastId( lastId, pageId );
    }

    @Override
    @Documented( "nodeMetaInconsistency" )
    public void nodeMetaInconsistency( long pageId, String message )
    {
        clean.setFalse();
        delegate.nodeMetaInconsistency( pageId, message );
    }

    @Override
    @Documented( "pageIdSeenMultipleTimes" )
    public void pageIdSeenMultipleTimes( long pageId )
    {
        clean.setFalse();
        delegate.pageIdSeenMultipleTimes( pageId );
    }

    @Override
    @Documented( "crashedPointer" )
    public void crashedPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB,
            long readPointerB, long pointerB, byte stateB )
    {
        clean.setFalse();
        delegate.crashedPointer( pageId, pointerType, generationA, readPointerA, pointerA, stateA, generationB, readPointerB, pointerB, stateB );
    }

    @Override
    @Documented( "brokenPointer" )
    public void brokenPointer( long pageId, GBPTreePointerType pointerType, long generationA, long readPointerA, long pointerA, byte stateA,
            long generationB,
            long readPointerB, long pointerB, byte stateB )
    {
        clean.setFalse();
        delegate.brokenPointer( pageId, pointerType, generationA, readPointerA, pointerA, stateA, generationB, readPointerB, pointerB, stateB );
    }

    @Override
    @Documented( "unreasonableKeyCount" )
    public void unreasonableKeyCount( long pageId, int keyCount )
    {
        clean.setFalse();
        delegate.unreasonableKeyCount( pageId, keyCount );
    }

    @Override
    @Documented( "childNodeFoundAmongParentNodes" )
    public void childNodeFoundAmongParentNodes( KeyRange<KEY> superRange, int level, long pageId )
    {
        clean.setFalse();
        delegate.childNodeFoundAmongParentNodes( superRange, level, pageId );
    }
}
