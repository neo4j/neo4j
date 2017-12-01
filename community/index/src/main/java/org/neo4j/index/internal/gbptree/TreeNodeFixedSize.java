package org.neo4j.index.internal.gbptree;

import org.neo4j.io.pagecache.PageCursor;

import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.read;
import static org.neo4j.index.internal.gbptree.TreeNode.Type.INTERNAL;

public class TreeNodeFixedSize<KEY,VALUE> extends TreeNode<KEY,VALUE>
{
    private final int internalMaxKeyCount;
    private final int leafMaxKeyCount;
    private final int keySize;
    private final int valueSize;

    TreeNodeFixedSize( int pageSize, Layout<KEY,VALUE> layout )
    {
        super( pageSize, layout );
        this.keySize = layout.keySize();
        this.valueSize = layout.valueSize();
        this.internalMaxKeyCount = Math.floorDiv( pageSize - (HEADER_LENGTH + SIZE_PAGE_REFERENCE),
                keySize + SIZE_PAGE_REFERENCE);
        this.leafMaxKeyCount = Math.floorDiv( pageSize - HEADER_LENGTH, keySize + valueSize );

        if ( internalMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException(
                    "For layout %s a page size of %d would only fit %d internal keys, minimum is 2",
                    layout, pageSize, internalMaxKeyCount );
        }
        if ( leafMaxKeyCount < 2 )
        {
            throw new MetadataMismatchException( "A page size of %d would only fit leaf keys, minimum is 2",
                    pageSize, leafMaxKeyCount );
        }
    }

    /**
     * Moves items (key/value/child) one step to the right, which means rewriting all items of the particular type
     * from pos - itemCount.
     * itemCount is keyCount for key and value, but keyCount+1 for children.
     */
    private static void insertSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int itemCount, int baseOffset,
            int itemSize )
    {
        for ( int posToMoveRight = itemCount - 1, offset = baseOffset + posToMoveRight * itemSize;
              posToMoveRight >= pos; posToMoveRight--, offset -= itemSize )
        {
            cursor.copyTo( offset, cursor, offset + itemSize * numberOfSlots, itemSize );
        }
    }

    private static void removeSlotAt( PageCursor cursor, int pos, int itemCount, int baseOffset, int itemSize )
    {
        for ( int posToMoveLeft = pos + 1, offset = baseOffset + posToMoveLeft * itemSize;
              posToMoveLeft < itemCount; posToMoveLeft++, offset += itemSize )
        {
            cursor.copyTo( offset, cursor, offset - itemSize, itemSize );
        }
    }

    private static int childSize()
    {
        return SIZE_PAGE_REFERENCE;
    }

    @Override
    KEY keyAt( PageCursor cursor, KEY into, int pos, Type type )
    {
        cursor.setOffset( keyOffset( pos ) );
        layout.readKey( cursor, into );
        return into;
    }

    // Insert key without associated value.
    // Useful for internal nodes and testing.
    @Override
    void insertKeyAt( PageCursor cursor, KEY key, int pos, int keyCount, Type type )
    {
        insertKeySlotsAt( cursor, pos, 1, keyCount );
        cursor.setOffset( keyOffset( pos ) );
        layout.writeKey( cursor, key );
    }

    @Override
    void insertKeyValueAt( PageCursor cursor, KEY key, VALUE value, int pos, int keyCount )
    {
        insertKeyAt( cursor, key, pos, keyCount, Type.LEAF );
        insertValueAt( cursor, value, pos, keyCount );
    }

    // Remove key without removing associated value.
    // Useful for internal nodes and testing.
    @Override
    void removeKeyAt( PageCursor cursor, int pos, int keyCount, Type type )
    {
        removeSlotAt( cursor, pos, keyCount, keyOffset( 0 ), keySize );
    }

    @Override
    void removeKeyValueAt( PageCursor cursor, int pos, int keyCount )
    {
        removeKeyAt( cursor, pos, keyCount, Type.LEAF );
        removeValueAt( cursor, pos, keyCount );
    }

    @Override
    void setKeyAt( PageCursor cursor, KEY key, int pos, Type type )
    {
        cursor.setOffset( keyOffset( pos ) );
        layout.writeKey( cursor, key );
    }

    @Override
    VALUE valueAt( PageCursor cursor, VALUE value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.readValue( cursor, value );
        return value;
    }

    @Override
    void setValueAt( PageCursor cursor, VALUE value, int pos )
    {
        cursor.setOffset( valueOffset( pos ) );
        layout.writeValue( cursor, value );
    }

    @Override
    long childAt( PageCursor cursor, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        return read( cursor, stableGeneration, unstableGeneration, pos );
    }

    @Override
    void insertChildAt( PageCursor cursor, long child, int pos, int keyCount,
            long stableGeneration, long unstableGeneration )
    {
        insertChildSlotsAt( cursor, pos, 1, keyCount );
        setChildAt( cursor, child, pos, stableGeneration, unstableGeneration );
    }

    @Override
    void removeChildAt( PageCursor cursor, int pos, int keyCount )
    {
        removeSlotAt( cursor, pos, keyCount + 1, childOffset( 0 ), childSize() );
    }

    @Override
    void setChildAt( PageCursor cursor, long child, int pos, long stableGeneration, long unstableGeneration )
    {
        cursor.setOffset( childOffset( pos ) );
        writeChild( cursor, child, stableGeneration, unstableGeneration );
    }

    private void insertKeyValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertKeySlotsAt( cursor, pos, numberOfSlots, keyCount );
        insertValueSlotsAt( cursor, pos, numberOfSlots, keyCount );
    }

    // Always insert together with key. Use insertKeyValueAt
    private void insertValueAt( PageCursor cursor, VALUE value, int pos, int keyCount )
    {
        insertValueSlotsAt( cursor, pos, 1, keyCount );
        setValueAt( cursor, value, pos );
    }

    // Always insert together with key. Use removeKeyValueAt
    private void removeValueAt( PageCursor cursor, int pos, int keyCount )
    {
        removeSlotAt( cursor, pos, keyCount, valueOffset( 0 ), valueSize );
    }

    private void insertKeySlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, keyOffset( 0 ), keySize );
    }

    private void insertValueSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount, valueOffset( 0 ), valueSize );
    }

    private void insertChildSlotsAt( PageCursor cursor, int pos, int numberOfSlots, int keyCount )
    {
        insertSlotsAt( cursor, pos, numberOfSlots, keyCount + 1, childOffset( 0 ), childSize() );
    }

    @Override
    int internalMaxKeyCount()
    {
        return internalMaxKeyCount;
    }

    @Override
    int leafMaxKeyCount()
    {
        return leafMaxKeyCount;
    }

    @Override
    boolean reasonableKeyCount( int keyCount )
    {
        return keyCount >= 0 && keyCount <= Math.max( internalMaxKeyCount(), leafMaxKeyCount() );
    }

    private int keyOffset( int pos )
    {
        return HEADER_LENGTH + pos * keySize;
    }

    private int valueOffset( int pos )
    {
        return HEADER_LENGTH + leafMaxKeyCount * keySize + pos * valueSize;
    }

    @Override
    int childOffset( int pos )
    {
        return HEADER_LENGTH + internalMaxKeyCount * keySize + pos * SIZE_PAGE_REFERENCE;
    }

    @Override
    String additionalToString()
    {
        return "keySize:" + keySize() + ", valueSize:" + valueSize;
    }

    private int keySize()
    {
        return keySize;
    }

    private int valueSize()
    {
        return valueSize;
    }

    @Override
    boolean internalOverflow( int keyCount )
    {
        return keyCount > internalMaxKeyCount();
    }

    @Override
    boolean leafOverflow( int keyCount )
    {
        return keyCount > leafMaxKeyCount();
    }

    @Override
    boolean leafUnderflow( int keyCount )
    {
        return keyCount < (leafMaxKeyCount() + 1) / 2;
    }

    @Override
    boolean canRebalanceLeaves( int leftKeyCount, int rightKeyCount )
    {
        return leftKeyCount + rightKeyCount >= leafMaxKeyCount();
    }

    @Override
    boolean canMergeLeaves( int leftKeyCount, int rightKeyCount )
    {
        return leftKeyCount + rightKeyCount <= leafMaxKeyCount();
    }

    @Override
    void doSplitLeaf( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos, KEY newKey,
            VALUE newValue, int middlePos )
    {
        if ( insertPos < middlePos )
        {
            //                  v-------v       copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,X,_,_,_,_,_,_,_
            // middle           ^
            copyKeysAndValues( leftCursor, middlePos - 1, rightCursor, 0, rightKeyCount );
            insertKeyValueAt( leftCursor, newKey, newValue, insertPos, middlePos - 1 );
        }
        else
        {
            //                  v---v           first copy
            //                        v-v       second copy
            // before _,_,_,_,_,_,_,_,_,_
            // insert _,_,_,_,_,_,_,_,X,_,_
            // middle           ^
            int countBeforePos = insertPos - middlePos;
            if ( countBeforePos > 0 )
            {
                // first copy
                copyKeysAndValues( leftCursor, middlePos, rightCursor, 0, countBeforePos );
            }
            insertKeyValueAt( rightCursor, newKey, newValue, countBeforePos, countBeforePos );
            int countAfterPos = leftKeyCount - insertPos;
            if ( countAfterPos > 0 )
            {
                // second copy
                copyKeysAndValues( leftCursor, insertPos, rightCursor, countBeforePos + 1, countAfterPos );
            }
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    @Override
    void doSplitInternal( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount, int insertPos, KEY newKey,
            long newRightChild, int middlePos, long stableGeneration, long unstableGeneration )
    {
        if ( insertPos < middlePos )
        {
            //                         v-------v       copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,X,_,_,_,_,_,_,_,_
            // insert child -,-,-,x,-,-,-,-,-,-,-,-
            // middle key              ^

            leftCursor.copyTo( keyOffset( middlePos ), rightCursor, keyOffset( 0 ), rightKeyCount * keySize() );
            leftCursor.copyTo( childOffset( middlePos ), rightCursor, childOffset( 0 ), (rightKeyCount + 1) * childSize() );
            insertKeyAt( leftCursor, newKey, insertPos, middlePos - 1, INTERNAL );
            insertChildAt( leftCursor, newRightChild, insertPos + 1, middlePos - 1, stableGeneration, unstableGeneration );
        }
        else
        {
            // pos > middlePos
            //                         v-v          first copy
            //                             v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,_,_,X,_,_,_
            // insert child -,-,-,-,-,-,-,-,x,-,-,-
            // middle key              ^

            // pos == middlePos
            //                                      first copy
            //                         v-v-v-v-v    second copy
            // before key    _,_,_,_,_,_,_,_,_,_
            // before child -,-,-,-,-,-,-,-,-,-,-
            // insert key    _,_,_,_,_,X,_,_,_,_,_
            // insert child -,-,-,-,-,-,x,-,-,-,-,-
            // middle key              ^

            // Keys
            int countBeforePos = insertPos - (middlePos + 1);
            // ... first copy
            if ( countBeforePos > 0 )
            {
                leftCursor.copyTo( keyOffset( middlePos + 1 ), rightCursor, keyOffset( 0 ), countBeforePos * keySize() );
            }
            // ... insert
            if ( countBeforePos >= 0 )
            {
                insertKeyAt( rightCursor, newKey, countBeforePos, countBeforePos, INTERNAL );
            }
            // ... second copy
            int countAfterPos = leftKeyCount - insertPos;
            if ( countAfterPos > 0 )
            {
                leftCursor.copyTo( keyOffset( insertPos ), rightCursor, keyOffset( countBeforePos + 1 ), countAfterPos * keySize() );
            }

            // Children
            countBeforePos = insertPos - middlePos;
            // ... first copy
            if ( countBeforePos > 0 )
            {
                // first copy
                leftCursor.copyTo( childOffset( middlePos + 1 ), rightCursor, childOffset( 0 ), countBeforePos * childSize() );
            }
            // ... insert
            insertChildAt( rightCursor, newRightChild, countBeforePos, countBeforePos, stableGeneration, unstableGeneration );
            // ... second copy
            if ( countAfterPos > 0 )
            {
                leftCursor.copyTo( childOffset( insertPos + 1 ), rightCursor, childOffset( countBeforePos + 1 ),
                        countAfterPos * childSize() );
            }
        }
        TreeNode.setKeyCount( leftCursor, middlePos );
        TreeNode.setKeyCount( rightCursor, rightKeyCount );
    }

    @Override
    void moveKeyValuesFromLeftToRight( PageCursor leftCursor, int leftKeyCount, PageCursor rightCursor, int rightKeyCount,
            int fromPosInLeftNode )
    {
        int numberOfKeysToMove = leftKeyCount - fromPosInLeftNode;

        // Push keys and values in right sibling to the right
        insertKeyValueSlotsAt( rightCursor, 0, numberOfKeysToMove, rightKeyCount );

        // Move keys and values from left sibling to right sibling
        copyKeysAndValues( leftCursor, fromPosInLeftNode, rightCursor, 0, numberOfKeysToMove );
    }

    private void copyKeysAndValues( PageCursor fromCursor, int fromPos, PageCursor toCursor, int toPos, int count )
    {
        fromCursor.copyTo( keyOffset( fromPos ), toCursor, keyOffset( toPos ), count * keySize() );
        fromCursor.copyTo( valueOffset( fromPos ), toCursor, valueOffset( toPos ),count * valueSize() );
    }
}
