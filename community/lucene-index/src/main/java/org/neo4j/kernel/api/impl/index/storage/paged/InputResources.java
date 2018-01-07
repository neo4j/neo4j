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
package org.neo4j.kernel.api.impl.index.storage.paged;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;
import java.util.LinkedList;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

/**
 * Lucene extensively uses {@link IndexInput#clone()} and
 * {@link IndexInput#slice(String, long, long)},
 * creating clone inputs. Those clones originate from some root input, the
 * input that came from {@link Directory#openInput(String, IOContext)}.
 * <p>
 * It apparently only closes the root inputs, so they become responsible for
 * closing resources associated with the clones. This resource tracking is
 * off-loaded here.
 * <p>
 * The root input has an instance of {@link RootInputResources}, and the clones
 * all share an instance of {@link CloneInputResources}.
 * <p>
 * Each input has it's own {@link PageCursor}, which needs closing, and all
 * inputs from the same root
 * share a {@link PagedFile}. Cursors are closed if/when the input associated
 * with the cursor is closed,
 * and everything still open is closed when the root input is closed.
 * <p>
 * <p>
 *
 * @see "ByteBufferIndexInput#close()"
 */
public interface InputResources
{

    class RootInputResources implements InputResources
    {
        private final PagedFile pagedFile;
        private final CloneInputResources cloneResources =
                new CloneInputResources( this );
        private LinkedList<PagedIndexInput> clones = new LinkedList<>();

        RootInputResources( PagedFile pagedFile )
        {
            this.pagedFile = pagedFile;
        }

        @Override
        public synchronized PageCursor openCursor( long pageId,
                PagedIndexInput owner ) throws IOException
        {
            // Called concurrently with #close(..)
            assert owner.cursor == null : String.format(
                    "Multiple cursors opened for the same input: %s", owner );

            if ( clones == null )
            {
                throw new IOException(
                        "This index has been closed, cannot open more inputs" );
            }

            PageCursor cursor =
                    pagedFile.io( pageId, PagedFile.PF_SHARED_READ_LOCK );

            // If this is a clone, track it
            if ( owner.resources != this )
            {
                clones.add( owner );
            }
            return cursor;
        }

        @Override
        public synchronized void close( PagedIndexInput input )
                throws IOException
        {
            // Note that this gets called concurrently with `openCursor`
            // if the index is closed while under load!
            assert input.resources == this : String.format(
                    "Closing input with unrelated resource tracker: " +
                            "%s closed by %s", input, this );

            if ( clones == null )
            {
                // Already closed
                return;
            }

            LinkedList<PagedIndexInput> localClones = this.clones;
            this.clones = null;

            // Close all clones
            IOException errorOnClose = null;
            for ( PagedIndexInput clone : localClones )
            {
                try
                {
                    clone.close();
                }
                catch ( IOException e )
                {
                    // Don't stop closing others because of an error
                    if ( errorOnClose == null )
                    {
                        errorOnClose = e;
                    }
                    else
                    {
                        errorOnClose.addSuppressed( e );
                    }
                }
            }

            // Close our own cursor
            try
            {
                input.cursor.close();
            }
            finally
            {
                input.cursor = null;
            }

            // And finally close the paged file
            pagedFile.close();

            if ( errorOnClose != null )
            {
                throw errorOnClose;
            }
        }

        @Override
        public InputResources cloneResources()
        {
            return cloneResources;
        }
    }

    class CloneInputResources implements InputResources
    {
        private final RootInputResources root;

        public CloneInputResources( RootInputResources root )
        {
            this.root = root;
        }

        @Override
        public PageCursor openCursor( long pageId, PagedIndexInput owner )
                throws IOException
        {
            return root.openCursor( pageId, owner );
        }

        @Override
        public void close( PagedIndexInput input )
        {
            assert input.resources == this : String.format(
                    "Closing input with unrelated resource tracker: " +
                            "%s closed by %s", input, this );
            if ( input.cursor == null )
            {
                // Already closed
                return;
            }

            // Close our cursor
            try
            {
                input.cursor.close();
            }
            finally
            {
                input.cursor = null;
            }
        }

        @Override
        public InputResources cloneResources()
        {
            return this;
        }
    }

    PageCursor openCursor( long pageId, PagedIndexInput owner )
            throws IOException;

    /**
     * Close the input. `input.resources == this` must be true. noop if already
     * closed.
     */
    void close( PagedIndexInput input ) throws IOException;

    /** Resource tracking for a clone input */
    InputResources cloneResources();
}
