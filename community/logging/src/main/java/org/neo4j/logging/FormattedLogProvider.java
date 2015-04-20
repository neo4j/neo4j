/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.logging;

import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static org.neo4j.logging.FormattedLog.OUTPUT_STREAM_CONVERTER;

/**
 * A {@link LogProvider} implementation that applies a simple formatting to each log message.
 */
public class FormattedLogProvider implements LogProvider
{
    private static final Pattern packagePattern = Pattern.compile( "(\\w)\\w+\\." );

    private final Supplier<PrintWriter> writerSupplier;
    private final boolean debugEnabled;
    private final boolean autoFlush;
    private final ConcurrentHashMap<String, Log> logs = new ConcurrentHashMap<>();

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to an {@link OutputStream}.
     *
     * @param out An {@link OutputStream} to write to
     * @return A {@link FormattedLogProvider} instance that writes to the specified OutputStream
     */
    public static FormattedLogProvider toOutputStream( OutputStream out )
    {
        return toOutputStream( out, false, true );
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to an {@link OutputStream}.
     *
     * @param out An {@link OutputStream} to write to
     * @param debugEnabled Enable writing of debug messages
     * @param autoFlush Flush the output after every log message
     * @return A {@link FormattedLogProvider} instance
     */
    public static FormattedLogProvider toOutputStream( OutputStream out, boolean debugEnabled, boolean autoFlush )
    {
        return new FormattedLogProvider( Suppliers.singleton( new PrintWriter( out ) ), debugEnabled, autoFlush );
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to {@link OutputStream}s obtained from the specified
     * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
     *
     * @param outSupplier A supplier for an output stream to write to
     * @return A {@link FormattedLogProvider} instance
     */
    public static FormattedLogProvider toOutputStream( Supplier<OutputStream> outSupplier )
    {
        return toOutputStream( outSupplier, false, true );
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to {@link OutputStream}s obtained from the specified
     * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
     *
     * @param outSupplier A supplier for an output stream to write to
     * @param debugEnabled Enable writing of debug messages
     * @param autoFlush Flush the output after every log message
     * @return A {@link FormattedLogProvider} instance
     */
    public static FormattedLogProvider toOutputStream( Supplier<OutputStream> outSupplier, boolean debugEnabled, boolean autoFlush )
    {
        return new FormattedLogProvider( Suppliers.adapted( outSupplier, OUTPUT_STREAM_CONVERTER ), debugEnabled, autoFlush );
    }

    protected FormattedLogProvider( Supplier<PrintWriter> writerSupplier, boolean debugEnabled, boolean autoFlush )
    {
        this.writerSupplier = writerSupplier;
        this.debugEnabled = debugEnabled;
        this.autoFlush = autoFlush;
    }

    @Override
    public Log getLog( Class loggingClass )
    {
        String shortenedClassName = packagePattern.matcher( loggingClass.getName() ).replaceAll( "$1." );
        return getLog( shortenedClassName );
    }

    @Override
    public Log getLog( String context )
    {
        Log log = logs.get( context );
        if ( log == null )
        {
            Log newLog = FormattedLog.toPrintWriter( writerSupplier, this, context, debugEnabled, autoFlush );
            log = logs.putIfAbsent( context, newLog );
            if ( log == null )
            {
                log = newLog;
            }
        }
        return log;
    }
}
