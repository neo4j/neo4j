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

import org.neo4j.kernel.impl.store.record.Abstract64BitRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;

/**
 * Represents a record ({@link NodeRecord}, {@link PropertyRecord}, ...) from a transaction log file with some
 * particular version.
 *
 * @param <R> the type of the record
 */
class LogRecord<R extends Abstract64BitRecord>
{
    private final R record;
    private final long logVersion;

    LogRecord( R record, long logVersion )
    {
        this.record = record;
        this.logVersion = logVersion;
    }

    R record()
    {
        return record;
    }

    long logVersion()
    {
        return logVersion;
    }

    @Override
    public String toString()
    {
        return record + " from log #" + logVersion;
    }
}
