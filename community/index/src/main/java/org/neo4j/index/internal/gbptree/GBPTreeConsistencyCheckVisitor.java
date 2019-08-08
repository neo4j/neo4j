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

public interface GBPTreeConsistencyCheckVisitor
{
    boolean clean();

    void notATreeNode( long pageId );

    void unknownTreeNodeType( long pageId, byte treeNodeType );

    void siblingsDontPointToEachOther(
            long leftNode,  long leftNodeGeneration,  long leftRightSiblingPointerGeneration, long leftRightSiblingPointer,
            long rightNode, long rightNodeGeneration, long rightLeftSiblingPointerGeneration, long rightLeftSiblingPointer );

    void rightmostNodeHasRightSibling( long rightmostNode, long rightSiblingPointer );

    void pointerToOldVersionOfTreeNode( long pageId, long successorPointer );

    void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointer,
            long pointerGeneration, long targetNodeGeneration );

    class Adaptor implements GBPTreeConsistencyCheckVisitor
    {
        @Override
        public boolean clean()
        {
            return true;
        }

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
                long rightNode, long rightNodeGeneration, long rightLeftSiblingPointerGeneration, long rightLeftSiblingPointer )
        {
        }

        @Override
        public void rightmostNodeHasRightSibling( long rightmostNode, long rightSiblingPointer )
        {
        }

        @Override
        public void pointerToOldVersionOfTreeNode( long pageId, long successorPointer )
        {
        }

        @Override
        public void pointerHasLowerGenerationThanNode( GBPTreePointerType pointerType, long sourceNode, long pointer, long pointerGeneration,
                long targetNodeGeneration )
        {
        }
    }
}
