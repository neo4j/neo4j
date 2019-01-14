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
package org.neo4j.logging;

import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * An abstract {@link Logger} implementation, which takes care of locking and flushing.
 */
public abstract class AbstractPrintWriterLogger implements Logger
{
    private final Supplier<PrintWriter> writerSupplier;
    private final Object lock;
    private final boolean autoFlush;

    /**
     * @param writerSupplier A {@link Supplier} for the {@link PrintWriter} that logs should be written to
     * @param lock           An object that will be used to synchronize all writes on
     * @param autoFlush      Whether to flush the writer after each log message is written
     */
    protected AbstractPrintWriterLogger( @Nonnull Supplier<PrintWriter> writerSupplier, @Nonnull Object lock,
            boolean autoFlush )
    {
        this.writerSupplier = writerSupplier;
        this.lock = lock;
        this.autoFlush = autoFlush;
    }

    @Override
    public void log( @Nonnull String message )
    {
        requireNonNull( message, "message must not be null" );
        PrintWriter writer;
        synchronized ( lock )
        {
            writer = writerSupplier.get();
            writeLog( writer, message );
        }
        maybeFlush( writer );
    }

    @Override
    public void log( @Nonnull String message, @Nonnull Throwable throwable )
    {
        requireNonNull( message, "message must not be null" );
        if ( throwable == null )
        {
            log( message );
            return;
        }
        PrintWriter writer;
        synchronized ( lock )
        {
            writer = writerSupplier.get();
            writeLog( writer, message, throwable );
        }
        maybeFlush( writer );
    }

    @Override
    public void log( @Nonnull String format, @Nullable Object... arguments )
    {
        requireNonNull( format, "format must not be null" );
        if ( arguments == null || arguments.length == 0 )
        {
            log( format );
            return;
        }
        String message = String.format( format, arguments );
        PrintWriter writer;
        synchronized ( lock )
        {
            writer = writerSupplier.get();
            writeLog( writer, message );
        }
        maybeFlush( writer );
    }

    @Override
    public void bulk( @Nonnull Consumer<Logger> consumer )
    {
        requireNonNull( consumer, "consumer must not be null" );
        PrintWriter writer;
        synchronized ( lock )
        {
            writer = writerSupplier.get();
            consumer.accept( getBulkLogger( writer, lock ) );
        }
        maybeFlush( writer );
    }

    /**
     * Invoked when a log line should be written. This method will only be called synchronously (whilst a lock is held
     * on the lock object provided during construction).
     *
     * @param writer the writer to write to
     * @param message the message to write
     */
    protected abstract void writeLog( @Nonnull PrintWriter writer, @Nonnull String message );

    /**
     * Invoked when a log line should be written. This method will only be called synchronously (whilst a lock is held
     * on the lock object provided during construction).
     *
     * @param writer the writer to write to
     * @param message the message to write
     * @param throwable the exception to append to the log message
     */
    protected abstract void writeLog( @Nonnull PrintWriter writer, @Nonnull String message,
            @Nonnull Throwable throwable );

    /**
     * Return a variant of the logger which will output to the specified writer (whilst holding a lock on the specified
     * object) in a bulk manner (no flushing, etc).
     *
     * @param writer the writer to write to
     * @param lock the object on which to lock
     * @return a new logger for bulk writes
     */
    protected abstract Logger getBulkLogger( @Nonnull PrintWriter writer, @Nonnull Object lock );

    private void maybeFlush( PrintWriter writer )
    {
        if ( autoFlush )
        {
            writer.flush();
        }
    }
}
