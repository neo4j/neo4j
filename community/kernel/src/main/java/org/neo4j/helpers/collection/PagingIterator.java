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
package org.neo4j.helpers.collection;

import java.util.Iterator;

/**
 * A {@link CachingIterator} which can more easily divide the items
 * into pages, where optionally each page can be seen as its own
 * {@link Iterator} instance for convenience using {@link #nextPage()}.
 * 
 * @author Mattias Persson
 *
 * @param <T> the type of items in this iterator.
 */
public class PagingIterator<T> extends CachingIterator<T>
{
    private final int pageSize;

    /**
     * Creates a new paging iterator with {@code source} as its underlying
     * {@link Iterator} to lazily get items from.
     * 
     * @param source the underlying {@link Iterator} to lazily get items from.
     * @param pageSize the max number of items in each page.
     */
    public PagingIterator( Iterator<T> source, int pageSize )
    {
        super( source );
        this.pageSize = pageSize;
    }
    
    /**
     * @return the page the iterator is currently at, starting a {@code 0}.
     * This value is based on the {@link #position()} and the page size.
     */
    public int page()
    {
        return position()/pageSize;
    }
    
    /**
     * Sets the current page of the iterator. {@code 0} means the first page.
     * @param newPage the current page to set for the iterator, must be
     * non-negative. The next item returned by the iterator will be the first
     * item in that page.
     * @return the page before changing to the new page.
     */
    public int page( int newPage )
    {
        int previousPage = page();
        position( newPage*pageSize );
        return previousPage;
    }
    
    /**
     * Returns a new {@link Iterator} instance which exposes the current page
     * as its own iterator, which fetches items lazily from the underlying
     * iterator. It is discouraged to use an {@link Iterator} returned from
     * this method at the same time as using methods like {@link #next()} or
     * {@link #previous()}, where the results may be unpredictable. So either
     * use only {@link #nextPage()} (in conjunction with {@link #page(int)} if
     * necessary) or go with regular {@link #next()}/{@link #previous()}.
     * 
     * @return the next page as an {@link Iterator}.
     */
    public Iterator<T> nextPage()
    {
        page( page() );
        return new PrefetchingIterator<T>()
        {
            private final int end = position()+pageSize;
            
            @Override
            protected T fetchNextOrNull()
            {
                if ( position() >= end )
                {
                    return null;
                }
                return PagingIterator.this.hasNext() ? PagingIterator.this.next() : null;
            }
        };
    }
}
