/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.nio.channels.ReadableByteChannel;
import java.util.List;

import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.xa.Command;
import org.neo4j.kernel.impl.util.StringLogger;

public class InterceptingXaLogicalLog extends XaLogicalLog
{
    private final XaDataSource ds;
    private TransactionInterceptor interceptor;

    public InterceptingXaLogicalLog( String fileName, XaResourceManager xaRm,
            XaCommandFactory cf, XaTransactionFactory xaTf,
            TransactionInterceptor interceptor, LogBufferFactory logBufferFactory, FileSystemAbstraction fileSystem, StringLogger stringLogger)
    {
        super( fileName, xaRm, cf, xaTf, logBufferFactory, fileSystem, stringLogger );
        this.interceptor = interceptor;
        this.ds = xaRm.getDataSource();
    }

    @Override
    protected LogDeserializer getLogDeserializer(
            ReadableByteChannel byteChannel )
    {
        LogDeserializer toReturn = new LogDeserializer( byteChannel )
        {
            @Override
            protected void intercept( List<LogEntry> entries )
            {
                for ( LogEntry entry : entries )
                {
                    if ( entry instanceof LogEntry.Command )
                    {
                        LogEntry.Command commandEntry = (LogEntry.Command) entry;
                        if ( commandEntry.getXaCommand() instanceof Command )
                        {
                            ( (Command) commandEntry.getXaCommand() ).accept( interceptor );
                        }
                    }
                    else if ( entry instanceof LogEntry.Start )
                    {
                        interceptor.setStartEntry( (LogEntry.Start) entry );
                    }
                    else if ( entry instanceof LogEntry.Commit )
                    {
                        interceptor.setCommitEntry( (LogEntry.Commit) entry );
                    }
                }
                interceptor.complete();
            }
        };
        return toReturn;
    }
}
