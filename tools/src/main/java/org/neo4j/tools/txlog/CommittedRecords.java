/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.tools.txlog;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

/**
 * Contains mapping from entity id ({@link NodeRecord#getId()}, {@link PropertyRecord#getId()}, ...) to
 * {@linkplain Abstract64BitRecord record} for records that have been previously seen during transaction log scan.
 * <p/>
 * Can determine if some given record is consistent with previously committed state.
 *
 * @param <R> the type of the record
 */
class CommittedRecords<R extends Abstract64BitRecord>
{
    private final CheckType<?,R> checkType;
    private final PrimitiveLongObjectMap<LogRecord<R>> recordsById;

    CommittedRecords( CheckType<?,R> check )
    {
        this.checkType = check;
        this.recordsById = Primitive.longObjectMap();
    }

    boolean isValid( R record )
    {
        LogRecord<R> current = recordsById.get( record.getId() );
        return current == null || checkType.equal( record, current.record() );
    }

    void put( R record, long logVersion )
    {
        recordsById.put( record.getId(), new LogRecord<>( record, logVersion ) );
    }

    LogRecord<R> get( long id )
    {
        return recordsById.get( id );
    }

    @Override
    public String toString()
    {
        return "CommittedRecords{" + "command=" + checkType.name() + ", recordsById.size=" + recordsById.size() + "}";
    }
}
