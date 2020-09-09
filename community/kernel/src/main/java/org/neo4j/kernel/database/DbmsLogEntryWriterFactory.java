/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.kernel.database;

import org.neo4j.dbms.database.TransactionLogVersionProvider;
import org.neo4j.io.fs.WritableChecksumChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter;
import org.neo4j.kernel.impl.transaction.log.entry.TransactionLogVersionSelector;

public class DbmsLogEntryWriterFactory implements LogEntryWriterFactory
{
    private final TransactionLogVersionProvider repository;

    public DbmsLogEntryWriterFactory( TransactionLogVersionProvider repository )
    {
        this.repository = repository;
    }

    @Override
    public <T extends WritableChecksumChannel> LogEntryWriter<T> createEntryWriter( T channel )
    {
        // Create a writer with a parser set matching the transaction log format version to use.
        return new LogEntryWriter<>( channel, TransactionLogVersionSelector.INSTANCE.select( repository.getVersion() ) );
    }
}
