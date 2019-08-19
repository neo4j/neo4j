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

import static org.neo4j.index.internal.gbptree.GBPTreeGenerationTarget.NO_GENERATION_TARGET;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.TreeNode.BYTE_POS_LEFTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNode.BYTE_POS_RIGHTSIBLING;
import static org.neo4j.index.internal.gbptree.TreeNode.BYTE_POS_SUCCESSOR;
import static org.neo4j.index.internal.gbptree.TreeNode.goTo;

final class GBPTreeCorruption
{
    private GBPTreeCorruption()
    {
    }

    /* PageCorruption */
    static <KEY, VALUE> PageCorruption<KEY,VALUE> crashed( GBPTreePointerType gbpTreePointerType )
    {
        return ( pageCursor, layout, node, treeState ) -> {
            int offset = gbpTreePointerType.offset( node );
            long stableGeneration = treeState.stableGeneration();
            long unstableGeneration = treeState.unstableGeneration();
            long crashGeneration = crashGeneration( treeState );
            pageCursor.setOffset( offset );
            long pointer = pointer( GenerationSafePointerPair.read( pageCursor, stableGeneration, unstableGeneration, NO_GENERATION_TARGET ) );
            overwriteGSPP( pageCursor, offset, crashGeneration, pointer );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> broken( GBPTreePointerType gbpTreePointerType )
    {
        return ( pageCursor, layout, node, treeState ) -> {
            int offset = gbpTreePointerType.offset( node );
            pageCursor.setOffset( offset );
            pageCursor.putInt( Integer.MAX_VALUE );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> setPointer( GBPTreePointerType pointerType, long pointer )
    {
        return ( cursor, layout, node, treeState ) -> {
            overwriteGSPP( cursor, pointerType.offset( node ), treeState.unstableGeneration(), pointer );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> notATreeNode()
    {
        return ( cursor, layout, node, treeState ) -> cursor.putByte( TreeNode.BYTE_POS_NODE_TYPE, Byte.MAX_VALUE );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> unknownTreeNodeType()
    {
        return ( cursor, layout, node, treeState ) -> cursor.putByte( TreeNode.BYTE_POS_TYPE, Byte.MAX_VALUE );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> rightSiblingPointToNonExisting()
    {
        return ( cursor, layout, node, treeState ) -> TreeNode
                .setRightSibling( cursor, GenerationSafePointer.MAX_POINTER, treeState.stableGeneration(), treeState.unstableGeneration() );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> leftSiblingPointToNonExisting()
    {
        return ( cursor, layout, node, treeState ) -> TreeNode
                .setLeftSibling( cursor, GenerationSafePointer.MAX_POINTER, treeState.stableGeneration(), treeState.unstableGeneration() );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> rightSiblingPointerHasTooLowGeneration()
    {
        return ( cursor, layout, node, treeState ) -> {
            long rightSibling = pointer( TreeNode.rightSibling( cursor, treeState.stableGeneration(), treeState.unstableGeneration() ) );
            overwriteGSPP( cursor, BYTE_POS_RIGHTSIBLING, GenerationSafePointer.MIN_GENERATION, rightSibling );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> leftSiblingPointerHasTooLowGeneration()
    {
        return ( cursor, layout, node, treeState ) -> {
            long leftSibling = pointer( TreeNode.leftSibling( cursor, treeState.stableGeneration(), treeState.unstableGeneration() ) );
            overwriteGSPP( cursor, BYTE_POS_LEFTSIBLING, GenerationSafePointer.MIN_GENERATION, leftSibling );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> childPointerHasTooLowGeneration( int childPos )
    {
        return ( cursor, layout, node, treeState ) -> {
            long child = pointer( node.childAt( cursor, childPos, treeState.stableGeneration(), treeState.unstableGeneration() ) );
            overwriteGSPP( cursor, node.childOffset( childPos ), GenerationSafePointer.MIN_GENERATION, child );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> setChild( int childPos, long childPointer )
    {
        return ( cursor, layout, node, treeState ) ->
                node.setChildAt( cursor, childPointer, childPos, treeState.stableGeneration(), treeState.unstableGeneration() );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> hasSuccessor()
    {
        return ( cursor, layout, node, treeState ) -> overwriteGSPP( cursor, BYTE_POS_SUCCESSOR, treeState.unstableGeneration(),
                GenerationSafePointer.MAX_POINTER );
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> swapKeyOrderLeaf( int firstKeyPos, int secondKeyPos, int keyCount )
    {
        return ( cursor, layout, node, treeState ) -> {
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
        return ( cursor, layout, node, treeState ) -> {
            // Remove key from higher position and insert into lower position
            int lowerKeyPos = firstKeyPos < secondKeyPos ? firstKeyPos : secondKeyPos;
            int higherKeyPos = firstKeyPos == lowerKeyPos ? secondKeyPos : firstKeyPos;

            // Record key and right child on higher position
            KEY key = layout.newKey();
            node.keyAt( cursor, key, higherKeyPos, TreeNode.Type.LEAF );
            long rightChild = node.childAt( cursor, higherKeyPos + 1, treeState.stableGeneration(), treeState.unstableGeneration() );

            // Remove key and right child, may need to defragment node to make sure we have room for insert later
            node.removeKeyAndRightChildAt( cursor, higherKeyPos, keyCount );
            node.defragmentLeaf( cursor );

            // Insert key and right child in lower position
            node.insertKeyAndRightChildAt( cursor, key, rightChild, lowerKeyPos, keyCount - 1, treeState.stableGeneration(), treeState.unstableGeneration() );
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> overwriteKeyAtPosLeaf( KEY key, int keyPos, int keyCount )
    {
        return ( cursor, layout, node, treeState ) -> {
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
        return ( cursor, layout, node, treeState ) -> {
            // Record rightChild so that we can reinsert it together with key later
            long rightChild = node.childAt( cursor, keyPos, treeState.stableGeneration(), treeState.unstableGeneration() );

            // Remove key and right child, may need to defragment node to make sure we have room for insert later
            node.removeKeyAndRightChildAt( cursor, keyPos, keyCount );
            node.defragmentLeaf( cursor );

            // Insert key and right child
            node.insertKeyAndRightChildAt( cursor, key, rightChild, keyPos, keyCount - 1, treeState.stableGeneration(), treeState.unstableGeneration() );
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> maximizeAllocOffsetInDynamicNode()
    {
        return ( cursor, layout, node, treeState ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            dynamicNode.setAllocOffset( cursor, cursor.getCurrentPageSize() ); // Clear alloc space
        };
    }

    static <KEY,VALUE> PageCorruption<KEY,VALUE> minimizeAllocOffsetInDynamicNode()
    {
        return ( cursor, layout, node, treeState ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            dynamicNode.setAllocOffset( cursor, TreeNodeDynamicSize.HEADER_LENGTH_DYNAMIC );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> decrementAllocOffsetInDynamicNode()
    {
        return ( cursor, layout, node, treeState ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            int allocOffset = dynamicNode.getAllocOffset( cursor );
            dynamicNode.setAllocOffset( cursor, allocOffset - 1 );
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> incrementDeadSpaceInDynamicNode()
    {
        return ( cursor, layout, node, treeState ) -> {
            TreeNodeDynamicSize dynamicNode = assertDynamicNode( node );
            int deadSpace = dynamicNode.getDeadSpace( cursor );
            dynamicNode.setDeadSpace( cursor, deadSpace + 1 );
        };
    }

    static <KEY,VALUE> IndexCorruption<KEY,VALUE> decrementFreelistWritePos()
    {
        return ( pagedFile, layout, node, treeState ) -> {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                goTo( cursor, "", treeState.pageId() );
                int decrementedWritePos = treeState.freeListWritePos() - 1;
                TreeState.write( cursor, treeState.stableGeneration(), treeState.unstableGeneration(), treeState.rootId(),
                        treeState.rootGeneration(), treeState.lastId(), treeState.freeListWritePageId(), treeState.freeListReadPageId(), decrementedWritePos,
                        treeState.freeListReadPos(), treeState.isClean() );
            }
        };
    }

    static <KEY, VALUE> IndexCorruption<KEY,VALUE> addFreelistEntry( long releasedId )
    {
        return ( pagedFile, layout, node, treeState ) -> {
            FreeListIdProvider freelist = getFreelist( pagedFile, treeState );
            freelist.releaseId( treeState.stableGeneration(), treeState.unstableGeneration(), releasedId );
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                goTo( cursor, "", treeState.pageId() );
                TreeState.write( cursor, treeState.stableGeneration(), treeState.unstableGeneration(), treeState.rootId(),
                        treeState.rootGeneration(), freelist.lastId(), freelist.writePageId(), freelist.readPageId(), freelist.writePos(),
                        freelist.readPos(), treeState.isClean() );
            }
        };
    }

    static <KEY,VALUE> IndexCorruption<KEY,VALUE> setTreeState( TreeState target )
    {
        return ( pagedFile, layout, node, treeState ) -> {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                goTo( cursor, "", treeState.pageId() ); // Write new tree state to current tree states page
                TreeState.write( cursor, target.stableGeneration(), target.unstableGeneration(), target.rootId(), target.rootGeneration(), target.lastId(),
                        target.freeListWritePageId(), target.freeListReadPageId(), target.freeListWritePos(), target.freeListReadPos(), target.isClean() );
            }
        };
    }

    static <KEY, VALUE> PageCorruption<KEY,VALUE> setKeyCount( int keyCount )
    {
        return ( cursor, layout, node, treeState ) -> {
            TreeNode.setKeyCount( cursor, keyCount );
        };
    }

    static <KEY,VALUE> IndexCorruption<KEY,VALUE> pageSpecificCorruption( long targetPage, PageCorruption<KEY,VALUE> corruption )
    {
        return (( pagedFile, layout, node, treeState ) -> {
            try ( PageCursor cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK ) )
            {
                goTo( cursor, "", targetPage );
                corruption.corrupt( cursor, layout, node, treeState );
            }
            ;
        });
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

    private static long crashGeneration( TreeState treeState )
    {
        if ( treeState.unstableGeneration() - treeState.stableGeneration() < 2 )
        {
            throw new IllegalStateException(
                    "Need stable and unstable generation to have a crash gap but was stableGeneration=" + treeState.stableGeneration() +
                            " and unstableGeneration=" + treeState.unstableGeneration() );
        }
        return treeState.unstableGeneration() - 1;
    }

    interface PageCorruption<KEY, VALUE>
    {
        void corrupt( PageCursor pageCursor, Layout<KEY,VALUE> layout, TreeNode<KEY,VALUE> node, TreeState treeState ) throws IOException;
    }

    interface IndexCorruption<KEY,VALUE>
    {
        void corrupt( PagedFile pagedFile, Layout<KEY,VALUE> layout, TreeNode<KEY,VALUE> node, TreeState treeState ) throws IOException;
    }
}
