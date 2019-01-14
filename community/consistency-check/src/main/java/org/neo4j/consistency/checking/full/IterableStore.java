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
package org.neo4j.consistency.checking.full;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.collection.BoundedIterable;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;

import static org.neo4j.consistency.checking.full.CloningRecordIterator.cloned;
import static org.neo4j.kernel.impl.store.Scanner.scan;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;

public class IterableStore<RECORD extends AbstractBaseRecord> implements BoundedIterable<RECORD>
{
    private final RecordStore<RECORD> store;
    private final boolean forward;
    private ResourceIterator<RECORD> iterator;

    public IterableStore( RecordStore<RECORD> store, boolean forward )
    {
        this.store = store;
        this.forward = forward;
    }

    @Override
    public long maxCount()
    {
        return store.getHighId();
    }

    @Override
    public void close()
    {
        closeIterator();
    }

    private void closeIterator()
    {
        if ( iterator != null )
        {
            iterator.close();
            iterator = null;
        }
    }

    @Override
    public Iterator<RECORD> iterator()
    {
        closeIterator();
        ResourceIterable<RECORD> iterable = scan( store, forward );
        return cloned( iterator = iterable.iterator() );
    }

    public void warmUpCache()
    {
        int recordsPerPage = store.getRecordsPerPage();
        long id = 0;
        long half = store.getHighId() / 2;
        RECORD record = store.newRecord();
        while ( id < half )
        {
            store.getRecord( id, record, FORCE );
            id += recordsPerPage - 1;
        }
    }
}
