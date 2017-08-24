/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.StandardOpenOption;

import org.neo4j.collection.primitive.PrimitiveLongCollections;
import org.neo4j.index.internal.gbptree.TreeNode.Section;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.test.rule.PageCacheAndDependenciesRule;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static org.neo4j.index.internal.gbptree.ConsistencyChecker.assertNoCrashOrBrokenPointerInGSPP;
import static org.neo4j.index.internal.gbptree.GenerationSafePointer.MIN_GENERATION;
import static org.neo4j.index.internal.gbptree.GenerationSafePointerPair.pointer;
import static org.neo4j.index.internal.gbptree.PageCursorUtil.goTo;
import static org.neo4j.index.internal.gbptree.TreeNodeSelector.selectHighestPrioritizedTreeNodeFormat;
import static org.neo4j.index.internal.gbptree.ValueMergers.overwrite;
import static org.neo4j.test.rule.PageCacheRule.config;

public class ConsistencyCheckerTest
{
    private static int pageSize = 1024;

    @Rule
    public PageCacheAndDependenciesRule rule = new PageCacheAndDependenciesRule( config().withPageSize( pageSize ) );

    protected TreeNode<MutableLong,MutableLong> node;
    protected Layout<MutableLong,MutableLong> layout;
    private SimpleIdProvider idProvider;
    private InternalTreeLogic<MutableLong,MutableLong> logic;
    private StructurePropagation<MutableLong> propagation;
    private final long oldStableGeneration = MIN_GENERATION;
    protected long stableGeneration = oldStableGeneration + 1;
    private final long crashGeneration = stableGeneration + 1;
    protected long unstableGeneration = crashGeneration + 1;
    private long root;
    private PagedFile pagedFile;
    protected PageCursor cursor;

    @Before
    public void before() throws IOException
    {
        PageCache pageCache = rule.pageCache();
        pagedFile = pageCache.map( rule.directory().file( "file" ), pageSize, StandardOpenOption.CREATE );
        cursor = pagedFile.io( 0, PagedFile.PF_SHARED_WRITE_LOCK );
        layout = new SimpleLongLayout();
        node = selectHighestPrioritizedTreeNodeFormat().instantiate( pageSize, layout );
        idProvider = new SimpleIdProvider();
        logic = new InternalTreeLogic<>( idProvider, node, layout );
        propagation = new StructurePropagation<>( layout.newKey(), layout.newKey(), layout.newKey() );
        root = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        node.goTo( cursor, "root", root );
        node.initializeLeaf( cursor, stableGeneration, unstableGeneration );
        logic.initialize( cursor );
    }

    @After
    public void after() throws IOException
    {
        cursor.close();
        pagedFile.close();
    }

    @Test
    public void shouldThrowDescriptiveExceptionOnBrokenGSPP() throws Exception
    {
        // GIVEN
        String pointerFieldName = "abc";
        long pointer = 123;

        cursor.next( 0 );
        node.initializeInternal( cursor, stableGeneration, crashGeneration );
        node.setSuccessor( cursor, pointer, stableGeneration, crashGeneration );

        // WHEN
        try
        {
            assertNoCrashOrBrokenPointerInGSPP( node, cursor, stableGeneration, unstableGeneration,
                    pointerFieldName, node.successorOffset() );
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
        MutableLong key = layout.newKey();
        for ( int g = 0, k = 0; g < 3; g++ )
        {
            for ( int i = 0; i < 100; i++, k++ )
            {
                key.setValue( k );
                logic.insert( cursor, propagation, key, key, ValueMergers.overwrite(),
                        stableGeneration, unstableGeneration );
                if ( propagation.hasRightKeyInsert )
                {
                    goTo( cursor, "new root",
                            idProvider.acquireNewId( stableGeneration, unstableGeneration ) );
                    node.initializeInternal( cursor, stableGeneration, unstableGeneration );
                    Section<MutableLong,MutableLong> mainSection = node.main();
                    mainSection.insertKeyAt( cursor, propagation.rightKey, 0, 0 );
                    mainSection.setKeyCount( cursor, 1 );
                    mainSection.setChildAt( cursor, propagation.midChild, 0, stableGeneration, unstableGeneration );
                    mainSection.setChildAt( cursor, propagation.rightChild, 1,
                            stableGeneration, unstableGeneration );
                    logic.initialize( cursor );
                }
                if ( propagation.hasMidChildUpdate )
                {
                    logic.initialize( cursor );
                }
                propagation.clear();
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
        catch ( TreeInconsistencyException e )
        {
            // THEN good
            assertThat( e.getMessage(), containsString( "unused pages" ) );
        }
    }

    @Test
    public void shouldDetectUnexpectedKeyCount() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 0, 0 );
        node.main().setKeyCount( cursor, node.main().leafMaxKeyCount() + 1 ); // one too many

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Unexpected main keyCount" ) );
        }
    }

    @Test
    public void shouldDetectOutOfOrderKeys() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 3 ), 1, 1 );
        node.main().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Non-unique key 3" ) );
        }
    }

    @Test
    public void shouldDetectNonUniqueKeys() throws Exception
    {
        // given
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 0, 0 );
        node.main().insertKeyAt( cursor, new MutableLong( 5 ), 1, 1 );
        node.main().setKeyCount( cursor, 2 );

        // when
        try
        {
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Non-unique key 5" ) );
        }
    }

    @Test
    public void shouldDetectInconsistentLeftSiblingPointer() throws Exception
    {
        // given
        //
        //            __ root__
        //           /         \
        //      siblingA--[R]->siblingB
        //                      /
        //     siblingC<-[L]<---
        //

        insertUntilRootSplit();
        logic.initialize( cursor );
        long siblingA = node.main().childAt( cursor, 0, stableGeneration, unstableGeneration );
        long siblingB = node.main().childAt( cursor, 1, stableGeneration, unstableGeneration );
        long siblingC = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        node.goTo( cursor, "sibling B", siblingB );
        node.setLeftSibling( cursor, siblingC, stableGeneration, unstableGeneration );

        // when
        try
        {
            node.goTo( cursor, "root", root );
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Sibling pointer does not align" ) );
        }
    }

    @Test
    public void shouldDetectInconsistentLeftSiblingPointerGeneration() throws Exception
    {
        // given
        //
        //            __ root__
        //           /         \
        //      siblingA<----->siblingB
        //

        insertUntilRootSplit();
        logic.initialize( cursor );
        long siblingA = node.main().childAt( cursor, 0, stableGeneration, unstableGeneration );
        long siblingB = node.main().childAt( cursor, 1, stableGeneration, unstableGeneration );
        node.goTo( cursor, "sibling B", siblingB );
        cursor.setOffset( node.leftSiblingOffset() );
        GenerationSafePointer.write( cursor, stableGeneration, pointer( siblingA ) );

        // when
        try
        {
            node.goTo( cursor, "root", root );
            new ConsistencyChecker<>( node, layout, unstableGeneration, unstableGeneration + 1 ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Sibling pointer generation differs" ) );
        }
    }

    @Test
    public void shouldDetectInconsistentRightSiblingPointer() throws Exception
    {
        // given
        //
        //            __ root__
        //           /         \
        //      siblingA<-[L]--siblingB
        //          \
        //           --[R]->siblingC
        //

        insertUntilRootSplit();
        logic.initialize( cursor );
        long siblingA = node.main().childAt( cursor, 0, stableGeneration, unstableGeneration );
        long siblingB = node.main().childAt( cursor, 1, stableGeneration, unstableGeneration );
        long siblingC = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        node.goTo( cursor, "sibling A", siblingA );
        node.setRightSibling( cursor, siblingC, stableGeneration, unstableGeneration );

        // when
        try
        {
            node.goTo( cursor, "root", root );
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Sibling pointer does not align" ) );
        }
    }

    @Test
    public void shouldDetectInconsistentRightSiblingPointerGeneration() throws Exception
    {
        // given
        //
        //            __ root__
        //           /         \
        //      siblingA<----->siblingB
        //

        insertUntilRootSplit();
        logic.initialize( cursor );
        long siblingA = node.main().childAt( cursor, 0, stableGeneration, unstableGeneration );
        long siblingB = node.main().childAt( cursor, 1, stableGeneration, unstableGeneration );
        node.goTo( cursor, "sibling A", siblingA );
        cursor.setOffset( node.rightSiblingOffset() );
        GenerationSafePointer.write( cursor, stableGeneration, pointer( siblingB ) );

        // when
        try
        {
            node.goTo( cursor, "root", root );
            new ConsistencyChecker<>( node, layout, unstableGeneration, unstableGeneration + 1 ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Sibling pointer generation differs" ) );
        }
    }

    @Test
    public void shouldDetectSuccessorInActiveLeafNode() throws Exception
    {
        // given
        long successor = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        node.setSuccessor( cursor, successor, stableGeneration, unstableGeneration );

        // when
        try
        {
            node.goTo( cursor, "root", root );
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Ended up on tree node" ) );
            assertThat( e.getMessage(), containsString( "which has successor" ) );
        }
    }

    @Test
    public void shouldDetectSuccessorInActiveInternalNode() throws Exception
    {
        insertUntilRootSplit();
        logic.initialize( cursor );
        long successor = idProvider.acquireNewId( stableGeneration, unstableGeneration );
        node.setSuccessor( cursor, successor, stableGeneration, unstableGeneration );

        // when
        try
        {
            node.goTo( cursor, "root", root );
            new ConsistencyChecker<>( node, layout, stableGeneration, unstableGeneration ).check( cursor, unstableGeneration );
            fail( "Should have failed" );
        }
        catch ( TreeInconsistencyException e )
        {
            // then
            assertThat( e.getMessage(), containsString( "Ended up on tree node" ) );
            assertThat( e.getMessage(), containsString( "which has successor" ) );
        }
    }

    private void insertUntilRootSplit() throws IOException
    {
        for ( long key = 0; !propagation.hasRightKeyInsert; key++ )
        {
            logic.insert( cursor, propagation, new MutableLong( key ), new MutableLong( key ),
                    overwrite(), stableGeneration, unstableGeneration );
        }
        root = logic.initializeNewRootAfterSplit( cursor, propagation, stableGeneration, unstableGeneration );
    }
}
