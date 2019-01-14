/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.tools.txlog;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.tools.txlog.checktypes.CheckType;

/**
 * Contains mapping from entity id ({@link NodeRecord#getId()}, {@link PropertyRecord#getId()}, ...) to
 * {@linkplain AbstractBaseRecord record} for records that have been previously seen during transaction log scan.
 * <p/>
 * Can determine if some given record is consistent with previously committed state.
 *
 * @param <R> the type of the record
 */
class CommittedRecords<R extends AbstractBaseRecord>
{
    private final CheckType<?,R> checkType;
    private final Map<Long,RecordInfo<R>> recordsById;

    CommittedRecords( CheckType<?,R> check )
    {
        this.checkType = check;
        this.recordsById = new HashMap<>();
    }

    public void put( R record, long logVersion, long txId )
    {
        recordsById.put( record.getId(), new RecordInfo<>( record, logVersion, txId ) );
    }

    public RecordInfo<R> get( long id )
    {
        return recordsById.get( id );
    }

    @Override
    public String toString()
    {
        return "CommittedRecords{" +
               "command=" + checkType.name() +
               ", recordsById.size=" + recordsById.size() + "}";
    }
}
