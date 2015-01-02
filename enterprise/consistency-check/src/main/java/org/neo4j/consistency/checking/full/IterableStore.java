/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.full;

import java.io.IOException;
import java.util.Iterator;

import org.neo4j.kernel.api.direct.BoundedIterable;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;

import static org.neo4j.kernel.impl.nioneo.store.RecordStore.IN_USE;

public class IterableStore<RECORD extends AbstractBaseRecord> implements BoundedIterable<RECORD>
{
    private final RecordStore<RECORD> store;

    public IterableStore( RecordStore<RECORD> store )
    {
        this.store = store;
    }

    @Override
    public long maxCount()
    {
        return store.getHighId();
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public Iterator<RECORD> iterator()
    {
        RecordStore.Processor<RuntimeException> processor = new RecordStore.Processor<RuntimeException>()
        {
        };
        return processor.scan( store, IN_USE ).iterator();
    }
}
