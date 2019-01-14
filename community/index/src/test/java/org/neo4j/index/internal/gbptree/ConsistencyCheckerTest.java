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

import org.apache.commons.lang3.mutable.MutableLong;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.index.internal.gbptree.ConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.goTo;
import static org.neo4j.index.internal.gbptree.SimpleLongLayout.longLayout;

public class ConsistencyCheckerTest
{
    @Test
    public void shouldThrowDescriptiveExceptionOnBrokenGSPP() throws Exception
    {
        // GIVEN
        int pageSize = 256;
        PageCursor cursor = new PageAwareByteArrayCursor( pageSize );
        long stableGeneration = MIN_GENERATION;
        long crashGeneration = stableGeneration + 1;
        long unstableGeneration = stableGeneration + 2;
        String pointerFieldName = "abc";
        long pointer = 123;

        cursor.next( 0 );
        new TreeNodeFixedSize<>( pageSize, longLayout().build() ).initializeInternal( cursor, stableGeneration, crashGeneration );
        TreeNode.setSuccessor( cursor, pointer, stableGeneration, crashGeneration );

        // WHEN
        try
        {
            assertNoCrashOrBrokenPointerInGSPP( cursor, stableGeneration, unstableGeneration,
                    pointerFieldName, TreeNode.BYTE_POS_SUCCESSOR );
            cursor.checkAndClearCursorException();
            fail( "Should have failed" );
        }
        catch ( CursorException e )
        {
            // THEN
            assertThat( e.getMessage(), containsString( pointerFieldName ) );
            assertThat( e.getMessage(), containsString( pointerFieldName ) );
            assertThat( e.getMessage(), containsString( "state=CRASH" ) );
            assertThat( e.getMessage(), containsString( "state=EMPTY" ) );
            assertThat( e.getMessage(), containsString( String.valueOf( pointer ) ) );
        }
    }

    @Test
    public void shouldDetectUnusedPages() throws Exception
    {
        // GIVEN
        int pageSize = 256;
        Layout<MutableLong,MutableLong> layout = longLayout().build();
        TreeNode<MutableLong,MutableLong> node = new TreeNodeFixedSize<>( pageSize, layout );
        long stableGeneration = GenerationSafePointer.MIN_GENERATION;
        long unstableGeneration = stableGeneration + 1;
        PageAwareByteArrayCursor cursor = new PageAwareByteArrayCursor( pageSize );
        SimpleIdProvider idProvider = new SimpleIdProvider( cursor::duplicate );
        InternalTreeLogic<MutableLong,MutableLong> logic = new InternalTreeLogic<>( idProvider, node, layout );
        cursor.next( idProvider.acquireNewId( stableGeneration, unstableGeneration ) );
        node.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        logic.initialize( cursor );
        StructurePropagation<MutableLong> structure = new StructurePropagation<>( layout.newKey(), layout.newKey(),
                layout.newKey() );
        MutableLong key = layout.newKey();
        for ( int g = 0, k = 0; g < 3; g++ )
        {
            for ( int i = 0; i < 100; i++, k++ )
            {
                key.setValue( k );
                logic.insert( cursor, structure, key, key, ValueMergers.overwrite(),
                        stableGeneration, unstableGeneration );
                if ( structure.hasRightKeyInsert )
                {
                    goTo( cursor, "new root",
                            idProvider.acquireNewId( stableGeneration, unstableGeneration ) );
                    node.initializeInternal( cursor, stableGeneration, unstableGeneration );
                    node.setChildAt( cursor, structure.midChild, 0, stableGeneration, unstableGeneration );
                    node.insertKeyAndRightChildAt( cursor, structure.rightKey, structure.rightChild, 0, 0,
                            stableGeneration, unstableGeneration );
                    TreeNode.setKeyCount( cursor, 1 );
                    logic.initialize( cursor );
                }
                if ( structure.hasMidChildUpdate )
                {
                    logic.initialize( cursor );
                }
                structure.clear();
            }
            stableGeneration = unstableGeneration;
            unstableGeneration++;
        }

        // WHEN
        ConsistencyChecker<MutableLong> cc =
                new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration );
        try
        {
            cc.checkSpace( cursor, idProvider.lastId(), PrimitiveLongCollections.emptyIterator() );
            fail( "Should have failed" );
        }
        catch ( RuntimeException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "unused pages" ) );
        }
    }
}
