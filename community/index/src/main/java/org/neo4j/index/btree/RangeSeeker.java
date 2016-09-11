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

public class RangeSeeker extends Seeker.CommonSeeker
{
    private final RangePredicate fromPred;
    private final RangePredicate toPred;
    private final CountPredicate countPred;
    private final boolean descending;
    private int resultCount = 0;

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred )
    {
        this( fromPred, toPred, false );
    }

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred, boolean descending )
    {
        this( fromPred, toPred, CountPredicate.NO_LIMIT, descending );
    }

    public RangeSeeker( RangePredicate fromPred, RangePredicate toPred, CountPredicate countPred, boolean descending )
    {
        this.fromPred = fromPred;
        this.toPred = toPred;
        this.descending = descending;
        this.countPred = countPred;
    }

    @Override
    protected void seekLeaf( PageCursor cursor, BTreeNode BTreeNode, List<SCResult> resultList ) throws IOException
    {
        int keyCount = BTreeNode.keyCount( cursor );

        if ( !descending )
        {
            int pos = 0;
            long[] key = BTreeNode.keyAt( cursor, pos );
            while ( pos < keyCount && fromPred.inRange( key ) < 0 )
            {
                pos++;
                key = BTreeNode.keyAt( cursor, pos );
            }

            while ( pos < keyCount && toPred.inRange( key ) <= 0 && !countPred.reachedLimit( resultCount ) )
            {
                SCKey SCKey = new SCKey( key[0], key[1] );
                long[] value = BTreeNode.valueAt( cursor, pos );
                SCValue SCValue = new SCValue( value[0], value[1] );
                resultList.add( new SCResult( SCKey, SCValue ) );
                resultCount++;
                pos++;
                key = BTreeNode.keyAt( cursor, pos );
            }

            if ( pos < keyCount || countPred.reachedLimit( resultCount ) )
            {
                return;
            }

            // Continue in right sibling
            long rightSibling = BTreeNode.rightSibling( cursor );
            if ( rightSibling != BTreeNode.NO_NODE_FLAG )
            {
                cursor.next( rightSibling );
                seekLeaf( cursor, BTreeNode, resultList );
            }
        }
        else
        {
            int pos = keyCount - 1;
            long[] key = BTreeNode.keyAt( cursor, pos );
            while ( pos > -1 && toPred.inRange( key ) > 0 )
            {
                pos--;
                if ( pos == -1 )
                {
                    break;
                }
                key = BTreeNode.keyAt( cursor, pos );
            }

            while ( pos > -1 && fromPred.inRange( key ) >= 0 && !countPred.reachedLimit( resultCount ) )
            {
                SCKey SCKey = new SCKey( key[0], key[1] );
                long[] value = BTreeNode.valueAt( cursor, pos );
                SCValue SCValue = new SCValue( value[0], value[1] );
                resultList.add( new SCResult( SCKey, SCValue ) );
                resultCount++;
                pos--;
                if ( pos == -1 )
                {
                    break;
                }
                key = BTreeNode.keyAt( cursor, pos );
            }

            if ( pos > -1 || countPred.reachedLimit( resultCount ) )
            {
                return;
            }

            // Continue in left sibling
            long leftSibling = BTreeNode.leftSibling( cursor );
            if ( leftSibling != BTreeNode.NO_NODE_FLAG )
            {
                cursor.next( leftSibling );
                seekLeaf( cursor, BTreeNode, resultList );
            }
        }
    }

    @Override
    protected void seekInternal( PageCursor cursor, BTreeNode BTreeNode, List<SCResult> resultList ) throws IOException
    {
        int keyCount = BTreeNode.keyCount( cursor );

        if ( !descending )
        {
            int pos = 0;
            long[] key = BTreeNode.keyAt( cursor, pos );
            while ( pos < keyCount && fromPred.inRange( key ) < 0 )
            {
                pos++;
                key = BTreeNode.keyAt( cursor, pos );
            }

            cursor.next( BTreeNode.childAt( cursor, pos ) );

            seek( cursor, BTreeNode, resultList );
        }
        else
        {
            int pos = keyCount - 1;
            long[] key = BTreeNode.keyAt( cursor, pos );
            while ( pos > -1 && toPred.inRange( key ) > 0 )
            {
                pos--;
                if ( pos == -1 )
                {
                    break;
                }
                key = BTreeNode.keyAt( cursor, pos );
            }

            cursor.next( BTreeNode.childAt( cursor, pos + 1 ) );

            seek( cursor, BTreeNode, resultList );
        }
    }
}
