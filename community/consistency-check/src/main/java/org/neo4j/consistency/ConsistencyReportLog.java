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
package org.neo4j.consistency;

import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

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

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return NullLogger.getInstance();
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return infoLogger;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return warnLogger;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return errorLogger;
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        PrintWriter writer;
        synchronized ( this )
        {
            writer = writerSupplier.get();
            consumer.accept( new ConsistencyReportLog( Suppliers.singleton( writer ), lock, false ) );
        }
    }
}
