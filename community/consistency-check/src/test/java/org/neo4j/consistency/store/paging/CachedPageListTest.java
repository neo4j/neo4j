/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.consistency.store.paging;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

public class CachedPageListTest
{
    @Test
    public void insertPagesByMovingThemFromNullListToTailOfARealList() throws Exception
    {
        // given
        CachedPageList list = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        Page page3 = new BasicPage();
        Page page4 = new BasicPage();
        Page page5 = new BasicPage();

        // when
        page1.moveToTailOf( list );
        page2.moveToTailOf( list );
        page3.moveToTailOf( list );
        page4.moveToTailOf( list );
        page5.moveToTailOf( list );

        // then
        assertEquals( 5, list.size() );

        assertSame( page1, list.head );
        assertSame( page5, list.tail );

        assertSame( list, page1.currentList );
        assertSame( list, page2.currentList );
        assertSame( list, page3.currentList );
        assertSame( list, page4.currentList );
        assertSame( list, page5.currentList );

        assertNull( page1.prevPage );
        assertSame( page1, page2.prevPage );
        assertSame( page2, page3.prevPage );
        assertSame( page3, page4.prevPage );
        assertSame( page4, page5.prevPage );

        assertSame( page2, page1.nextPage );
        assertSame( page3, page2.nextPage );
        assertSame( page4, page3.nextPage );
        assertSame( page5, page4.nextPage );
        assertNull( page5.nextPage );
    }

    @Test
    public void moveSingletonToEmptyList() throws Exception
    {
        // given
        CachedPageList list1 = new CachedPageList();
        CachedPageList list2 = new CachedPageList();
        Page page = new BasicPage();
        page.moveToTailOf( list1 );

        // when
        page.moveToTailOf( list2 );

        // then
        assertEquals( 0, list1.size() );
        assertNull( list1.head );
        assertNull( list1.tail );

        assertEquals( 1, list2.size() );
        assertSame( page, list2.head );
        assertSame( page, list2.tail );
        assertSame( list2, page.currentList );
        assertNull( page.prevPage );
        assertNull( page.nextPage );
    }

    @Test
    public void moveHeadOfTwoPageListToEmptyList() throws Exception
    {
        // given
        CachedPageList list1 = new CachedPageList();
        CachedPageList list2 = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        page1.moveToTailOf( list1 );
        page2.moveToTailOf( list1 );

        // when
        page1.moveToTailOf( list2 );

        // then
        assertEquals( 1, list1.size() );
        assertSame( page2, list1.head );
        assertSame( page2, list1.tail );
        assertSame( list1, page2.currentList );
        assertNull( page2.prevPage );
        assertNull( page2.nextPage );

        assertEquals( 1, list2.size() );
        assertSame( page1, list2.head );
        assertSame( page1, list2.tail );
        assertSame( list2, page1.currentList );
        assertNull( page1.prevPage );
        assertNull( page1.nextPage );
    }

    @Test
    public void moveTailOfTwoPageListToEmptyList() throws Exception
    {
        // given
        CachedPageList list1 = new CachedPageList();
        CachedPageList list2 = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        page1.moveToTailOf( list1 );
        page2.moveToTailOf( list1 );

        // when
        page2.moveToTailOf( list2 );

        // then
        assertEquals( 1, list1.size() );
        assertSame( page1, list1.head );
        assertSame( page1, list1.tail );
        assertSame( list1, page1.currentList );
        assertNull( page1.prevPage );
        assertNull( page1.nextPage );

        assertEquals( 1, list2.size() );
        assertSame( page2, list2.head );
        assertSame( page2, list2.tail );
        assertSame( list2, page2.currentList );
        assertNull( page2.prevPage );
        assertNull( page2.nextPage );
    }

    @Test
    public void moveMiddleOfThreePageListToEmptyList() throws Exception
    {
        // given
        CachedPageList list1 = new CachedPageList();
        CachedPageList list2 = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        Page page3 = new BasicPage();
        page1.moveToTailOf( list1 );
        page2.moveToTailOf( list1 );
        page3.moveToTailOf( list1 );

        // when
        page2.moveToTailOf( list2 );

        // then
        assertEquals( 2, list1.size() );
        assertSame( page1, list1.head );
        assertSame( page3, list1.tail );
        assertSame( list1, page1.currentList );
        assertSame( list1, page3.currentList );
        assertNull( page1.prevPage );
        assertSame( page1, page3.prevPage );
        assertSame( page3, page1.nextPage );
        assertNull( page3.nextPage );

        assertEquals( 1, list2.size() );
        assertSame( page2, list2.head );
        assertSame( page2, list2.tail );
        assertSame( list2, page2.currentList );
        assertNull( page2.prevPage );
        assertNull( page2.nextPage );
    }

    @Test
    public void moveMiddleOfLongListToEmptyList() throws Exception
    {
        // given
        CachedPageList list1 = new CachedPageList();
        CachedPageList list2 = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        Page page3 = new BasicPage();
        Page page4 = new BasicPage();
        Page page5 = new BasicPage();
        page1.moveToTailOf( list1 );
        page2.moveToTailOf( list1 );
        page3.moveToTailOf( list1 );
        page4.moveToTailOf( list1 );
        page5.moveToTailOf( list1 );

        // when
        page3.moveToTailOf( list2 );

        // then
        assertEquals( 4, list1.size() );
        assertSame( page1, list1.head );
        assertSame( page5, list1.tail );
        assertSame( list1, page1.currentList );
        assertSame( list1, page2.currentList );
        assertSame( list1, page4.currentList );
        assertSame( list1, page5.currentList );
        assertNull( page1.prevPage );
        assertSame( page1, page2.prevPage );
        assertSame( page2, page4.prevPage );
        assertSame( page4, page5.prevPage );
        assertSame( page2, page1.nextPage );
        assertSame( page4, page2.nextPage );
        assertSame( page5, page4.nextPage );
        assertNull( page5.nextPage );

        assertEquals( 1, list2.size() );
        assertSame( page3, list2.head );
        assertSame( page3, list2.tail );
        assertSame( list2, page3.currentList );
        assertNull( page3.prevPage );
        assertNull( page3.nextPage );
    }

    @Test
    public void moveMiddleOfLongListToNonEmptyList() throws Exception
    {
        // given
        CachedPageList list1 = new CachedPageList();
        CachedPageList list2 = new CachedPageList();

        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        Page page3 = new BasicPage();
        Page page4 = new BasicPage();
        Page page5 = new BasicPage();
        page1.moveToTailOf( list1 );
        page2.moveToTailOf( list1 );
        page3.moveToTailOf( list1 );
        page4.moveToTailOf( list1 );
        page5.moveToTailOf( list1 );

        Page page6 = new BasicPage();
        Page page7 = new BasicPage();
        page6.moveToTailOf( list2 );
        page7.moveToTailOf( list2 );

        // when
        page3.moveToTailOf( list2 );

        // then
        assertEquals( 4, list1.size() );
        assertSame( page1, list1.head );
        assertSame( page5, list1.tail );
        assertSame( list1, page1.currentList );
        assertSame( list1, page2.currentList );
        assertSame( list1, page4.currentList );
        assertSame( list1, page5.currentList );
        assertNull( page1.prevPage );
        assertSame( page1, page2.prevPage );
        assertSame( page2, page4.prevPage );
        assertSame( page4, page5.prevPage );
        assertSame( page2, page1.nextPage );
        assertSame( page4, page2.nextPage );
        assertSame( page5, page4.nextPage );
        assertNull( page5.nextPage );

        assertEquals( 3, list2.size() );
        assertSame( page6, list2.head );
        assertSame( page3, list2.tail );
        assertSame( list2, page6.currentList );
        assertSame( list2, page7.currentList );
        assertSame( list2, page3.currentList );
        assertNull( page6.prevPage );
        assertSame( page6, page7.prevPage );
        assertSame( page7, page3.prevPage );
        assertSame( page7, page6.nextPage );
        assertSame( page3, page7.nextPage );
        assertNull( page3.nextPage );
    }

    @Test
    public void shouldRemoveHeadOfSingletonList() throws Exception
    {
        // given
        CachedPageList list = new CachedPageList();
        Page page = new BasicPage();
        page.moveToTailOf( list );

        // when
        Page removedPage = list.removeHead();

        // then
        assertSame( page, removedPage );
        assertEquals( 0, list.size() );
        assertNull( list.head );
        assertNull( list.tail );

        assertNull( page.currentList );
        assertNull( page.prevPage );
        assertNull( page.nextPage );
    }

    @Test
    public void shouldRemoveHeadOfTwoPageList() throws Exception
    {
        // given
        CachedPageList list = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        page1.moveToTailOf( list );
        page2.moveToTailOf( list );

        // when
        Page removedPage = list.removeHead();

        // then
        assertSame( page1, removedPage );
        assertEquals( 1, list.size() );
        assertSame( page2, list.head );
        assertSame( page2, list.tail );

        assertNull( page1.currentList );
        assertNull( page1.prevPage );
        assertNull( page1.nextPage );

        assertSame( list, page2.currentList );
        assertNull( page2.prevPage );
        assertNull( page2.nextPage );
    }

    @Test
    public void shouldRemoveHeadOfLongList() throws Exception
    {
        // given
        CachedPageList list = new CachedPageList();
        Page page1 = new BasicPage();
        Page page2 = new BasicPage();
        Page page3 = new BasicPage();
        page1.moveToTailOf( list );
        page2.moveToTailOf( list );
        page3.moveToTailOf( list );

        // when
        Page removedPage = list.removeHead();

        // then
        assertSame( page1, removedPage );
        assertEquals( 2, list.size() );
        assertSame( page2, list.head );
        assertSame( page3, list.tail );

        assertNull( page1.currentList );
        assertNull( page1.prevPage );
        assertNull( page1.nextPage );

        assertSame( list, page2.currentList );
        assertSame( list, page3.currentList );
        assertNull( page2.prevPage );
        assertSame( page2, page3.prevPage );
        assertSame( page3, page2.nextPage );
        assertNull( page3.nextPage );
    }

    static class BasicPage extends Page<Integer>
    {
        @Override
        protected void evict( Integer payload )
        {
        }

        @Override
        protected void hit()
        {
        }
    }
}
