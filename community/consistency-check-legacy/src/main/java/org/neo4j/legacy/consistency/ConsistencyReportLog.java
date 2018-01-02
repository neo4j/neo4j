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
package org.neo4j.legacy.consistency;

import java.io.PrintWriter;

import org.neo4j.function.Consumer;
import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.logging.NullLogger;

public class ConsistencyReportLog extends AbstractLog
{
    private final Supplier<PrintWriter> writerSupplier;
    private final Object lock;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    public ConsistencyReportLog( Supplier<PrintWriter> writerSupplier )
    {
        this( writerSupplier, null, true );
    }

    private ConsistencyReportLog( Supplier<PrintWriter> writerSupplier, Object maybeLock, boolean autoFlush )
    {
        this.writerSupplier = writerSupplier;
        this.lock = ( maybeLock != null ) ? maybeLock : this;
        infoLogger = new ConsistencyReportLogger( writerSupplier, lock, "INFO ", autoFlush );
        warnLogger = new ConsistencyReportLogger( writerSupplier, lock, "WARN ", autoFlush );
        errorLogger = new ConsistencyReportLogger( writerSupplier, lock, "ERROR", autoFlush );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return false;
    }

    @Override
    public Logger debugLogger()
    {
        return NullLogger.getInstance();
    }

    @Override
    public Logger infoLogger()
    {
        return infoLogger;
    }

    @Override
    public Logger warnLogger()
    {
        return warnLogger;
    }

    @Override
    public Logger errorLogger()
    {
        return errorLogger;
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        PrintWriter writer;
        synchronized (this)
        {
            writer = writerSupplier.get();
            consumer.accept( new ConsistencyReportLog( Suppliers.singleton( writer ), lock, false ) );
        }
    }
}
