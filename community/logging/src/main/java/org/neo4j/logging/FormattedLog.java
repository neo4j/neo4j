/**
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

import org.neo4j.function.Consumer;
import org.neo4j.function.Function;
import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static java.util.Objects.*;

/**
 * A {@link Log} implementation that applies a simple formatting to each log message.
 */
public class FormattedLog extends AbstractLog
{
    static final Function<OutputStream, PrintWriter> OUTPUT_STREAM_CONVERTER = new Function<OutputStream, PrintWriter>() {
        @Override
        public PrintWriter apply( OutputStream outputStream )
        {
            return new PrintWriter( new OutputStreamWriter( outputStream, UTF_8 ) );
        }
    };
    private static final TimeZone UTC = TimeZone.getTimeZone( "UTC" );
    private static final Charset UTF_8 = Charset.forName( "UTF-8" );

    private final Supplier<PrintWriter> writerSupplier;
    private final Object lock;
    private final String category;
    private final boolean debugEnabled;
    private final boolean autoFlush;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    /**
     * @param out An {@link OutputStream} to write to
     * @return A {@link FormattedLog} instance that writes to the specified OutputStream
     */
    public static FormattedLog toOutputStream( OutputStream out )
    {
        return toOutputStream( Suppliers.singleton( out ), null, null, false, true );
    }

    /**
     * Creates a {@link FormattedLog} instance that writes messages to {@link OutputStream}s obtained from the specified
     * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
     *
     * @param outSupplier A supplier for an output stream to write to
     * @param lock An object used to synchronize each write
     * @param category A String to be written into each log message (may be empty or null)
     * @param debugEnabled Enable writing of debug messages
     * @param autoFlush Flush the output after every log message
     * @return A {@link FormattedLog} instance
     */
    public static FormattedLog toOutputStream( Supplier<OutputStream> outSupplier, Object lock, String category, boolean debugEnabled, boolean autoFlush )
    {
        return new FormattedLog( Suppliers.adapted( outSupplier, OUTPUT_STREAM_CONVERTER ), lock, category, debugEnabled, autoFlush );
    }

    /**
     * @param writer A {@link Writer} to write to
     * @return A {@link FormattedLog} instance that writes to the specified OutputStream
     */
    public static FormattedLog toWriter( Writer writer )
    {
        return toPrintWriter( new PrintWriter( writer ) );
    }

    /**
     * @param writer A {@link PrintWriter} to write to
     * @return A {@link FormattedLog} instance that writes to the specified OutputStream
     */
    public static FormattedLog toPrintWriter( PrintWriter writer )
    {
        return toPrintWriter( Suppliers.singleton( writer ), null, null, false, true );
    }

    /**
     * @param writerSupplier A supplier for a {@link Writer} to write to
     * @param lock An object used to synchronize each write
     * @param category A String to be written into each log message (may be empty or null)
     * @param debugEnabled Enable writing of debug messages
     * @param autoFlush Flush the output after every log message
     * @return A {@link FormattedLog} instance that writes to the specified OutputStream
     */
    public static FormattedLog toPrintWriter( Supplier<PrintWriter> writerSupplier, Object lock, String category, boolean debugEnabled, boolean autoFlush )
    {
        return new FormattedLog( writerSupplier, lock, category, debugEnabled, autoFlush );
    }

    protected FormattedLog( Supplier<PrintWriter> writerSupplier, Object maybeLock, String category, boolean debugEnabled, boolean autoFlush )
    {
        this.writerSupplier = writerSupplier;
        this.lock = ( maybeLock != null ) ? maybeLock : this;
        this.category = category;
        this.debugEnabled = debugEnabled;
        this.autoFlush = autoFlush;

        String debugPrefix = ( category != null && !category.isEmpty() ) ? "DEBUG [" + category + "]" : "DEBUG";
        String infoPrefix = ( category != null && !category.isEmpty() ) ? "INFO  [" + category + "]" : "INFO ";
        String warnPrefix = ( category != null && !category.isEmpty() ) ? "WARN  [" + category + "]" : "WARN ";
        String errorPrefix = ( category != null && !category.isEmpty() ) ? "ERROR [" + category + "]" : "ERROR";

        this.debugLogger = ( debugEnabled ) ? new FormattedLogger( writerSupplier, lock, debugPrefix, autoFlush ) : NullLogger.getInstance();
        this.infoLogger = new FormattedLogger( writerSupplier, lock, infoPrefix, autoFlush );
        this.warnLogger = new FormattedLogger( writerSupplier, lock, warnPrefix, autoFlush );
        this.errorLogger = new FormattedLogger( writerSupplier, lock, errorPrefix, autoFlush );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debugEnabled;
    }

    @Override
    public Logger debugLogger()
    {
        return this.debugLogger;
    }

    @Override
    public Logger infoLogger()
    {
        return this.infoLogger;
    }

    @Override
    public Logger warnLogger()
    {
        return this.warnLogger;
    }

    @Override
    public Logger errorLogger()
    {
        return this.errorLogger;
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        PrintWriter writer;
        synchronized (lock)
        {
            writer = writerSupplier.get();
            consumer.accept( new FormattedLog( Suppliers.singleton( writer ), lock, category, debugEnabled, false ) );
        }
        if ( autoFlush )
        {
            writer.flush();
        }
    }

    public static class FormattedLogger extends AbstractPrintWriterLogger
    {
        private final String prefix;
        private final DateFormat format;

        public FormattedLogger( Supplier<PrintWriter> writerSupplier, Object lock, String prefix, boolean autoFlush )
        {
            super( writerSupplier, lock, autoFlush );
            this.prefix = prefix;
            format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSS" );
            format.setTimeZone( UTC );
        }

        @Override
        protected void writeLog( PrintWriter out, String message )
        {
            requireNonNull( message, "message" );

            lineStart( out );
            out.write( message );
            out.println();
        }

        @Override
        protected void writeLog( PrintWriter out, String message, Throwable throwable )
        {
            requireNonNull( message, "message" );

            lineStart( out );
            out.write( message );
            if ( throwable != null )
            {
                if ( throwable.getMessage() != null )
                {
                    out.write( ' ' );
                    out.write( throwable.getMessage() );
                }
                out.println();
                throwable.printStackTrace( out );
            }
        }

        @Override
        protected void writeLog( PrintWriter out, String format, Object[] arguments )
        {
            requireNonNull( format, "format" );

            String message = String.format( format, arguments );
            lineStart( out );
            out.write( message );
            out.println();
        }

        @Override
        protected Logger getBulkLogger( PrintWriter out, Object lock )
        {
            return new FormattedLogger( Suppliers.singleton( out ), lock, prefix, false );
        }

        private void lineStart( PrintWriter out )
        {
            out.write( time() );
            out.write( ' ' );
            out.write( prefix );
            out.write( ' ' );
        }

        private String time()
        {
            return format.format( new Date() );
        }
    }
}
