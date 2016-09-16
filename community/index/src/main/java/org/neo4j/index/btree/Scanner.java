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
package org.neo4j.index.btree;

import java.io.IOException;
import java.util.List;

import org.neo4j.index.SCKey;
import org.neo4j.index.SCResult;
import org.neo4j.index.SCValue;
import org.neo4j.index.Seeker;
import org.neo4j.io.pagecache.PageCursor;

public class Scanner extends Seeker.CommonSeeker
{
    @Override
    protected void seekLeaf( PageCursor cursor, TreeNode bTreeNode, List<SCResult> resultList ) throws IOException
    {
        while ( true )
        {
            int keyCount = bTreeNode.keyCount( cursor );
            for ( int i = 0; i < keyCount; i++ )
            {
                long[] key = bTreeNode.keyAt( cursor, new long[2], i );
                long[] value = bTreeNode.valueAt( cursor, new long[2], i );
                resultList.add( new SCResult( new SCKey( key[0], key[1] ), new SCValue( value[0], value[1] ) ) );
            }
            long rightSibling = bTreeNode.rightSibling( cursor );
            if ( !bTreeNode.isNode( rightSibling ) )
            {
                break;
            }
            cursor.next( rightSibling );
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, TreeNode BTreeNode, List<SCResult> resultList ) throws IOException
    {
        cursor.next( BTreeNode.childAt( cursor, 0 ) );
        seek( cursor, BTreeNode, resultList );
    }
}
