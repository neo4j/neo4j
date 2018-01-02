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
/**
 * <h1>The Neo4j PageCache API</h1>
 *
 * <p>
 *     This package contains the API for the page caching mechanism used in Neo4j.
 *     How to acquire a concrete implementation of the API depends on the implementation in question.
 *     The Kernel implements its own mechanism to seek out and instantiate implementations of this API,
 *     based on the database configuration.
 * </p>
 *
 * <h2>Page Caching Concepts</h2>
 *
 * <p>
 *     The purpose of a page cache is to cache data from files on a storage device,
 *     and keep the most often used data in memory where access is fast.
 *     This duplicates the most popular data from the file, into memory.
 *     Assuming that not all data can fit in memory (even though it sometimes can),
 *     the least used data will then be pushed out of memory,
 *     when we need data that is not already in the cache.
 *     This is called eviction, and choosing what to evict is the responsibility of
 *     the eviction algorithm that runs inside the page cache implementation.
 * </p>
 *
 * <p>
 *     A file must first have to be "mapped" into the page cache, before the page cache
 *     can cache the contents of the files.
 *     When you no longer have an immediate use for the contents of the file, it can be
 *     "unmapped."
 *     Mapping a file using the
 *     {@link org.neo4j.io.pagecache.PageCache#map(java.io.File, int, java.nio.file.OpenOption...) map} method gives
 *     you a {@link org.neo4j.io.pagecache.PagedFile} object, through which the
 *     contents of the file can be accessed.
 *     Once a file has been mapped with the page cache, it should no longer be accessed
 *     directly through the file system, because the page cache will keep changes in
 *     memory, thinking it is managing the only authoritative copy.
 * </p>
 *
 * <p>
 *     If a file is mapped more than once, the same <code>PagedFile</code> is returned,
 *     and its reference counter is incremented. Unmapping decrements the reference
 *     counter, discarding the PagedFile from the cache if the counter reaches zero.
 *     If the last reference was unmapped, then all dirty pages for that file will be
 *     flushed before the file is discarded from the cache.
 * </p>
 *
 * <p>
 *     A "page" is a space that can fit a quantity of data, and is part of a larger whole.
 *     This larger whole can either be a file, or the memory allocated for the page cache.
 *     We refer to these two types of pages as "file pages" and "cache pages" respectively.
 *     Pages are the unit of what data is popular or not, and the unit of moving data into
 *     memory, and out to storage. When a cache page is holding the contents of a file
 *     page, the two are said to be "bound" to one another.
 * </p>
 *
 * <p>
 *     Each <code>PagedFile</code> object has a translation table, that logically translate
 *     file page ids for the given file, into cache page ids. The concrete implementations
 *     are typically more like Maps where the keys are the file page ids, and the values
 *     are concrete page object that currently holds that particular file page.
 * </p>
 *
 * <p>
 *     File pages are typically sized as a multiple of the size of the records they
 *     contain, so that you are guaranteed to be able to read or write a record in full,
 *     whenever you pin a page.
 *     File pages should be as large as they can possibly be, while still being no larger
 *     than the cache page size.
 *     Then the <code>filePageId</code> can be computed based on the
 *     <code>recordId</code> as the integer division <code>recordId / recordsPerPage</code>
 *     while the offset into the page is the modulo of that same division.
 * </p>
 *
 * <p>
 *     If a file page is not in memory, but someone needs it, a page fault occurs.
 *     Page faulting is finding a free page, and swapping the contents of the given file
 *     page into it. This has to be done in a thread-safe way, because multiple threads
 *     may race to discover that a page they want is not in memory, and this may be the
 *     same page. Page faulting also has to update the translation table, which again is
 *     something that needs to be done in a thread-safe manner. Page faulting also needs
 *     to take races with eviction into consideration, as the pages are now transitioning
 *     from free to bound, and eviction is a process that transition a page from bound to
 *     free.
 * </p>
 *
 * <p>
 *     If there are no, or not enough, free pages, then eviction occurs. Each page has a
 *     usage stamp, that is incremented on access and decremented by the dedicated
 *     eviction thread. If the counter reaches zero, the page is evicted. If the page was
 *     dirty because it had received writes since it was faulted, it will then be flushed
 *     before it is evicted and added back to the list of free pages.
 * </p>
 *
 * <p>
 *     Knowledge of how to move file pages in and out of cache pages is contained in a so
 *     called {@link org.neo4j.io.pagecache.PageSwapper}. The <code>Page</code>
 *     implementations themselves know how to do IO that moves data in and out of their
 *     respective memory area, but it is the swapper that tells them what file to use for
 *     IO, where in that file the data is located, and how much data needs to be moved.
 *     Every <code>PagedFile</code> have their own dedicated <code>PageSwapper</code>,
 *     that is instantiated for the given file by the
 *     {@link org.neo4j.io.pagecache.PageSwapperFactory}.
 * </p>
 *
 * <p>
 *     Once a file has been mapped, and a <code>PagedFile</code> object made available,
 *     the {@link org.neo4j.io.pagecache.PagedFile#io(long, int) io method} can be used
 *     to interact with the contents of the file. It takes in an initial file page id and
 *     a bitmap of intentions, such as what locking behaviour to use, and returns a
 *     {@link org.neo4j.io.pagecache.PageCursor} object. The <code>PageCursor</code> is
 *     the window into the data managed by the page cache.
 * </p>
 *
 * <p>
 *     Initially, the <code>PageCursor</code> is not bound to any page. Calling the
 *     {@link org.neo4j.io.pagecache.PageCursor#next()} method on the cursor will
 *     advance it to its next page. The first page that the cursor binds to, is the
 *     page with the file page id given to the <code>io</code> method. From then on,
 *     the cursor will scan linearly through the file.
 * </p>
 *
 * <p>
 *     The <code>next</code> method returns <code>true</code> if it successfully bound
 *     to the next page in its sequence. This is usually the case, but when
 *     {@link org.neo4j.io.pagecache.PagedFile#PF_SHARED_LOCK} or
 *     {@link org.neo4j.io.pagecache.PagedFile#PF_NO_GROW} is specified, the
 *     <code>next</code> method will return <code>false</code> if the cursor would
 *     otherwise move beyond the end of the file.
 * </p>
 *
 * <p>
 *     The <code>next</code> will grab the desired lock on the page (as specified by
 *     the <code>pf_flags</code> argument to the <code>io</code> method call) on the page,
 *     and then we can do the IO we intended. Following the IO, the
 *     {@link org.neo4j.io.pagecache.PageCursor#shouldRetry()} method must be consulted,
 *     and the IO must be redone on the page if it returns true. This is best done in a
 *     <code>do-while</code> loop. This retrying allows some optimistic optimisations in
 *     the page cache, that improves performance on average.
 * </p>
 *
 * <p>
 *     Here's a logical overview of a page cache:
 * </p>
 *
 * <pre><code>
 *     +---------------[ PageCache ]-----------------------------------+
 *     |                                                               |
 *     |  * PageSwapperFactory{ FileSystemAbstraction }                |
 *     |  * evictionThread                                             |
 *     |  * a large collection of Page objects:                        |
 *     |                                                               |
 *     |  +---------------[ Page ]----------------------------------+  |
 *     |  |                                                         |  |
 *     |  |  * usageCounter                                         |  |
 *     |  |  * some kind of read/write lock                         |  |
 *     |  |  * a cache page sized buffer                            |  |
 *     |  |  * binding metadata{ filePageId, PageSwapper }          |  |
 *     |  |                                                         |  |
 *     |  +---------------------------------------------------------+  |
 *     |                                                               |
 *     |  * linked list of mapped PagedFile instances:                 |
 *     |                                                               |
 *     |  +--------------[ PagedFile ]------------------------------+  |
 *     |  |                                                         |  |
 *     |  |  * referenceCounter                                     |  |
 *     |  |  * PageSwapper{ StoreChannel, filePageSize }            |  |
 *     |  |  * PageCursor freelists                                 |  |
 *     |  |  * translation table:                                   |  |
 *     |  |                                                         |  |
 *     |  |  +--------------[ translation table ]----------------+  |  |
 *     |  |  |                                                   |  |  |
 *     |  |  |  A translation table is basically a map from      |  |  |
 *     |  |  |  file page ids to Page objects. It is updated     |  |  |
 *     |  |  |  concurrently by page faulters and the eviction   |  |  |
 *     |  |  |  thread.                                          |  |  |
 *     |  |  |                                                   |  |  |
 *     |  |  +---------------------------------------------------+  |  |
 *     |  +---------------------------------------------------------+  |
 *     +---------------------------------------------------------------+
 *
 *     +--------------[ PageCursor ]-----------------------------------+
 *     |                                                               |
 *     |  * currentPage: Page                                          |
 *     |  * page lock metadata                                         |
 *     |                                                               |
 *     +---------------------------------------------------------------+
 * </code></pre>
 *
 */
package org.neo4j.io.pagecache;
