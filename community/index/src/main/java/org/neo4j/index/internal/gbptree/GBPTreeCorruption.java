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

import java.io.IOException;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNode.BYTE_POS_LEFTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNode.BYTE_POS_RIGHTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNode.BYTE_POS_SUCCESSOR;

final class GBPTreeCorruption
{
    private GBPTreeCorruption()
    {
    }

    /* PageCorruption */
    static <KEY, VALUE> PageCorruption<KEY,VALUE> crashed( GBPTreePointerType gbpTreePointerType )
    {
        return ( pagedFile, pageCursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            pageCursor.setOffset( gbpTreePointerType.offset( node ) );
            GenerationSafePointerPair.write( pageCursor, 42, stableGeneration, crashGeneration );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> notATreeNode()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> cursor.putByte( TreeNode.BYTE_POS_NODE_TYPE, Byte.MAX_VALUE );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> unknownTreeNodeType()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> cursor.putByte( TreeNode.BYTE_POS_TYPE, Byte.MAX_VALUE );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> rightSiblingPointToNonExisting()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> TreeNode
                .setRightSibling( cursor, GenerationSafePointer.MAX_POINTER, stableGeneration, unstableGeneration );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> leftSiblingPointToNonExisting()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> TreeNode
                .setLeftSibling( cursor, GenerationSafePointer.MAX_POINTER, stableGeneration, unstableGeneration );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> rightSiblingPointerHasTooLowGeneration()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            long rightSibling = pointer( TreeNode.rightSibling( cursor, stableGeneration, unstableGeneration ) );
            overwriteGSPP( cursor, BYTE_POS_RIGHTSIBLING, GenerationSafePointer.MIN_GENERATION, rightSibling );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> leftSiblingPointerHasTooLowGeneration()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            long leftSibling = pointer( TreeNode.leftSibling( cursor, stableGeneration, unstableGeneration ) );
            overwriteGSPP( cursor, BYTE_POS_LEFTSIBLING, GenerationSafePointer.MIN_GENERATION, leftSibling );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> childPointerHasTooLowGeneration( int childPos )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            long child = pointer( node.childAt( cursor, childPos, stableGeneration, unstableGeneration ) );
            overwriteGSPP( cursor, node.childOffset( childPos ), GenerationSafePointer.MIN_GENERATION, child );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> hasSuccessor()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> overwriteGSPP( cursor, BYTE_POS_SUCCESSOR, unstableGeneration,
                GenerationSafePointer.MAX_POINTER );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> swapKeyOrderLeaf( int firstKeyPos, int secondKeyPos, int keyCount )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            // Remove key from higher position and insert into lower position
            int lowerKeyPos = firstKeyPos < secondKeyPos ? firstKeyPos : secondKeyPos;
            int higherKeyPos = firstKeyPos == lowerKeyPos ? secondKeyPos : firstKeyPos;

            // Record key and value on higher position
            KEY key = layout.newKey();
            VALUE value = layout.newValue();
            node.keyAt( cursor, key, higherKeyPos, TreeNode.Type.LEAF );
            node.valueAt( cursor, value, higherKeyPos );

            // Remove key and value, may need to defragment node to make sure we have room for insert later
            node.removeKeyValueAt( cursor, higherKeyPos, keyCount );
            node.defragmentLeaf( cursor );

            // Insert key and value in lower position
            node.insertKeyValueAt( cursor, key, value, lowerKeyPos, keyCount - 1 );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> swapKeyOrderInternal( int firstKeyPos, int secondKeyPos, int keyCount )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            // Remove key from higher position and insert into lower position
            int lowerKeyPos = firstKeyPos < secondKeyPos ? firstKeyPos : secondKeyPos;
            int higherKeyPos = firstKeyPos == lowerKeyPos ? secondKeyPos : firstKeyPos;

            // Record key and right child on higher position
            KEY key = layout.newKey();
            node.keyAt( cursor, key, higherKeyPos, TreeNode.Type.LEAF );
            long rightChild = node.childAt( cursor, higherKeyPos + 1, stableGeneration, unstableGeneration );

            // Remove key and right child, may need to defragment node to make sure we have room for insert later
            node.removeKeyAndRightChildAt( cursor, higherKeyPos, keyCount );
            node.defragmentLeaf( cursor );

            // Insert key and right child in lower position
            node.insertKeyAndRightChildAt( cursor, key, rightChild, lowerKeyPos, keyCount - 1, stableGeneration, unstableGeneration );
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> overwriteKeyAtPosLeaf( KEY key, int keyPos, int keyCount )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            // Record value so that we can reinsert it together with key later
            VALUE value = layout.newValue();
            node.valueAt( cursor, value, keyPos );

            // Remove key and value, may need to defragment node to make sure we have room for insert later
            node.removeKeyValueAt( cursor, keyPos, keyCount );
            node.defragmentLeaf( cursor );

            // Insert new key and value
            node.insertKeyValueAt( cursor, key, value, keyPos, keyCount - 1 );
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> overwriteKeyAtPosInternal( KEY key, int keyPos, int keyCount )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            // Record rightChild so that we can reinsert it together with key later
            long rightChild = node.childAt( cursor, keyPos, stableGeneration, unstableGeneration );

            // Remove key and right child, may need to defragment node to make sure we have room for insert later
            node.removeKeyAndRightChildAt( cursor, keyPos, keyCount );
            node.defragmentLeaf( cursor );

            // Insert key and right child
            node.insertKeyAndRightChildAt( cursor, key, rightChild, keyPos, keyCount - 1, stableGeneration, unstableGeneration );
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> maximizeAllocOffsetInDynamicNode()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            dynamicNode.setAllocOffset( cursor, cursor.getCurrentPageSize() ); // Clear alloc space
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> minimizeAllocOffsetInDynamicNode()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            dynamicNode.setAllocOffset( cursor, TreeNodeDynamicSize.HEADER_LENGTH_DYNAMIC );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> decrementAllocOffsetInDynamicNode()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            int allocOffset = dynamicNode.getAllocOffset( cursor );
            dynamicNode.setAllocOffset( cursor, allocOffset - 1 );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> incrementDeadSpaceInDynamicNode()
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            int deadSpace = dynamicNode.getDeadSpace( cursor );
            dynamicNode.setDeadSpace( cursor, deadSpace + 1 );
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> decrementFreelistWritePos( TreeState treeState )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            int decrementedWritePos = treeState.freeListWritePos() - 1;
            TreeState.write( cursor, treeState.stableGeneration(), treeState.unstableGeneration(), treeState.rootId(),
                    treeState.rootGeneration(), treeState.lastId(), treeState.freeListWritePageId(), treeState.freeListReadPageId(), decrementedWritePos,
                    treeState.freeListReadPos(), treeState.isClean() );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> addFreelistEntry( TreeState treeState, long releasedId )
    {
        return ( pagedFile, cursor, layout, node, stableGeneration, unstableGeneration, crashGeneration ) -> {
            FreeListIdProvider freelist = getFreelist( pagedFile, treeState );
            freelist.releaseId( stableGeneration, unstableGeneration, releasedId );
            TreeState.write( cursor, treeState.stableGeneration(), treeState.unstableGeneration(), treeState.rootId(),
                    treeState.rootGeneration(), freelist.lastId(), freelist.writePageId(), freelist.readPageId(), freelist.writePos(),
                    freelist.readPos(), treeState.isClean() );
        };
    }

    private static FreeListIdProvider getFreelist( PagedFile pagedFile, TreeState treeState )
    {
        FreeListIdProvider freelist = new FreeListIdProvider( pagedFile, pagedFile.pageSize(), treeState.lastId(), FreeListIdProvider.NO_MONITOR );
        freelist.initialize( treeState.lastId(), treeState.freeListWritePageId(), treeState.freeListReadPageId(), treeState.freeListWritePos(),
                freelist.readPos() );
        return freelist;
    }

    private static <KEY, VALUE> TreeNodeDynamicSize assertDynamicNode( TreeNode<KEY,VALUE> node )
    {
        if ( !(node instanceof TreeNodeDynamicSize) )
        {
            throw new RuntimeException( "Can not use this corruption if node is not of type " + TreeNodeDynamicSize.class.getSimpleName() );
        }
        return (TreeNodeDynamicSize) node;
    }

    private static void overwriteGSPP( PageCursor cursor, int gsppOffset, long generation, long pointer )
    {
        cursor.setOffset( gsppOffset );
        GenerationSafePointer.write( cursor, generation, pointer );
        GenerationSafePointer.clean( cursor );
    }

    interface PageCorruption<KEY, VALUE>
    {
        void corrupt( PagedFile pagedFile, PageCursor pageCursor, Layout<KEY,VALUE> layout, TreeNode<KEY,VALUE> node,
                long stableGeneration, long unstableGeneration, long crashGeneration ) throws IOException;
    }
}
