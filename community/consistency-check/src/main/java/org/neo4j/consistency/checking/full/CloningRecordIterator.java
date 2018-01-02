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
package org.neo4j.consistency.checking.full;

import java.util.Iterator;

import org.neo4j.graphdb.Resource;
import org.neo4j.helpers.collection.PrefetchingResourceIterator;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

public class CloningRecordIterator<R extends AbstractBaseRecord> extends PrefetchingResourceIterator<R>
{
    private final Iterator<R> actualIterator;

    public CloningRecordIterator( Iterator<R> actualIterator )
    {
        this.actualIterator = actualIterator;
    }

    @Override
    @SuppressWarnings( "unchecked" )
    protected R fetchNextOrNull()
    {
        return actualIterator.hasNext() ? (R) actualIterator.next().clone() : null;
    }

    @Override
    public void close()
    {
        if ( actualIterator instanceof Resource )
        {
            ((Resource)actualIterator).close();
        }
    }

    public static <R extends AbstractBaseRecord> Iterator<R> cloned( Iterator<R> iterator )
    {
        return new CloningRecordIterator<>( iterator );
    }
}
