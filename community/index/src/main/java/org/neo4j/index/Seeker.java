/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.index;

import java.io.IOException;
import java.util.List;

import org.neo4j.index.btree.TreeNode;
import org.neo4j.io.pagecache.PageCursor;

public interface Seeker
{
    /**
     * Cursor will be moved from page.
     * @param cursor        {@link PageCursor} pinned to page with node (internal or leaf)
     * @param resultList    {@link java.util.List} where found results will be stored
     * @throws IOException  on cursor failure
     */
    void seek( PageCursor cursor, TreeNode bTreeNode, List<SCResult> resultList ) throws IOException;

    public abstract class CommonSeeker implements Seeker
    {

        // TODO: A lot of time is spent in the seek method, both for seek and scan. Can we make it faster?
        // TODO: Maybe with binary search in IndexSearch.
        @Override
        public void seek( PageCursor cursor, TreeNode BTreeNode, List<SCResult> resultList ) throws IOException
        {
            if ( BTreeNode.isInternal( cursor ) )
            {
                seekInternal( cursor, BTreeNode, resultList );
            }
            else if ( BTreeNode.isLeaf( cursor ) )
            {
                seekLeaf( cursor, BTreeNode, resultList );
            }
            else
            {
                throw new IllegalStateException( "node reported type other than internal or leaf" );
            }
        }

        protected abstract void seekLeaf( PageCursor cursor, TreeNode BTreeNode, List<SCResult> resultList ) throws IOException;

        protected abstract void seekInternal( PageCursor cursor, TreeNode BTreeNode, List<SCResult> resultList ) throws IOException;
    }
}
