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

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

/**
 * Represents a record ({@link NodeRecord}, {@link PropertyRecord}, ...) from a transaction log file with some
 * particular version.
 *
 * @param <R> the type of the record
 */
public class RecordInfo<R extends AbstractBaseRecord>
{
    private final R record;
    private final long logVersion;
    private final long txId;

    public RecordInfo( R record, long logVersion, long txId )
    {
        this.record = record;
        this.logVersion = logVersion;
        this.txId = txId;
    }

    public R record()
    {
        return record;
    }

    public long txId()
    {
        return txId;
    }

    long logVersion()
    {
        return logVersion;
    }

    @Override
    public String toString()
    {
        return String.format( "%s (log:%d txId:%d)", record, logVersion, txId );
    }
}
