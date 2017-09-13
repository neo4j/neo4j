/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import org.neo4j.function.Suppliers;

/**
 * A {@link Log} implementation that applies a simple formatting to each log message.
 */
public class FormattedLog extends AbstractLog
{
    static final Function<OutputStream, PrintWriter> OUTPUT_STREAM_CONVERTER =
            outputStream -> new PrintWriter( new OutputStreamWriter( outputStream, StandardCharsets.UTF_8 ) );

    /**
     * A Builder for a {@link FormattedLog}
     */
    public static class Builder
    {
        private ZoneId zoneId = ZoneOffset.UTC;
        private Object lock = this;
        private String category;
        private Level level = Level.INFO;
        private boolean autoFlush = true;
        private DateTimeFormatter dateTimeFormatter = FormattedLogger.DATE_TIME_FORMATTER;
        private Supplier<ZonedDateTime> dateTimeFormatterSupplier = () ->
                FormattedLogger.DEFAULT_CURRENT_DATE_TIME.apply( zoneId );

        private Builder()
        {
        }

        /**
         * Set the zoneId for datestamps in the log
         *
         * @return this builder
         */
        public Builder withUTCZoneId()
        {
            return withZoneId( ZoneOffset.UTC );
        }

        /**
         * Set the zoneId for datestamps in the log
         *
         * @return this builder
         * @param timezone
         * @deprecated use {@link #withZoneId(ZoneId)}
         */
        @Deprecated
        public Builder withTimeZone( TimeZone timezone )
        {
            return this.withZoneId( timezone.toZoneId() );
        }

        /**
         * Set the zoneId for datestamps in the log
         *
         * @param zoneId
         * @return this builder
         */
        public Builder withZoneId( ZoneId zoneId )
        {
            this.zoneId = zoneId;
            return this;
        }

        /**
         * Set the dateFormat for datestamps in the log
         *
         * @param dateTimeFormatter the dateFormat to use for datestamps
         * @return this builder
         */
        public Builder withDateTimeFormatter( DateTimeFormatter dateTimeFormatter )
        {
            this.dateTimeFormatter = dateTimeFormatter;
            return this;
        }

        /**
         * Use the specified object to synchronize on.
         *
         * @param lock the object to synchronize on
         * @return this builder
         */
        public Builder usingLock( Object lock )
        {
            this.lock = lock;
            return this;
        }

        /**
         * Include the specified category in each output log line.
         *
         * @param category the category to include ing each output line
         * @return this builder
         */
        public Builder withCategory( String category )
        {
            this.category = category;
            return this;
        }

        /**
         * Use the specified log {@link Level} as a default.
         *
         * @param level the log level to use as a default
         * @return this builder
         */
        public Builder withLogLevel( Level level )
        {
            this.level = level;
            return this;
        }

        /**
         * Use the specified function
         *
         * @param zonedDateTimeSupplier the log level to use as a default
         * @return this builder
         */
        Builder withTimeSupplier( Supplier<ZonedDateTime> zonedDateTimeSupplier )
        {
            this.dateTimeFormatterSupplier = zonedDateTimeSupplier;
            return this;
        }

        /**
         * Disable auto flushing.
         *
         * @return this builder
         */
        public Builder withoutAutoFlush()
        {
            autoFlush = false;
            return this;
        }

        /**
         * Creates a {@link FormattedLog} instance that writes messages to an {@link OutputStream}.
         *
         * @param out An {@link OutputStream} to write to
         * @return A {@link FormattedLog} instance that writes to the specified OutputStream
         */
        public FormattedLog toOutputStream( OutputStream out )
        {
            return toPrintWriter( Suppliers.singleton( OUTPUT_STREAM_CONVERTER.apply( out ) ) );
        }

        /**
         * Creates a {@link FormattedLog} instance that writes messages to {@link OutputStream}s obtained from the specified
         * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
         *
         * @param outSupplier A supplier for an output stream to write to
         * @return A {@link FormattedLog} instance
         */
        public FormattedLog toOutputStream( Supplier<OutputStream> outSupplier )
        {
            return toPrintWriter( Suppliers.adapted( outSupplier, OUTPUT_STREAM_CONVERTER ) );
        }

        /**
         * Creates a {@link FormattedLog} instance that writes messages to a {@link Writer}.
         *
         * @param writer A {@link Writer} to write to
         * @return A {@link FormattedLog} instance that writes to the specified Writer
         */
        public FormattedLog toWriter( Writer writer )
        {
            return toPrintWriter( new PrintWriter( writer ) );
        }

        /**
         * Creates a {@link FormattedLog} instance that writes messages to a {@link PrintWriter}.
         *
         * @param writer A {@link PrintWriter} to write to
         * @return A {@link FormattedLog} instance that writes to the specified PrintWriter
         */
        public FormattedLog toPrintWriter( PrintWriter writer )
        {
            return toPrintWriter( Suppliers.singleton( writer ) );
        }

        /**
         * Creates a {@link FormattedLog} instance that writes messages to {@link PrintWriter}s obtained from the specified
         * {@link Supplier}. The PrintWriter is obtained from the Supplier before every log message is written.
         *
         * @param writerSupplier A supplier for a {@link PrintWriter} to write to
         * @return A {@link FormattedLog} instance that writes to the specified PrintWriter
         */
        public FormattedLog toPrintWriter( Supplier<PrintWriter> writerSupplier )
        {
            return new FormattedLog( writerSupplier, zoneId, lock, category, level, autoFlush,
                    dateTimeFormatter, dateTimeFormatterSupplier );
        }
    }

    private final Supplier<PrintWriter> writerSupplier;
    final ZoneId zoneId;
    final Object lock;
    private final String category;
    private final AtomicReference<Level> levelRef;
    final boolean autoFlush;
    private final Logger debugLogger;
    private final Logger infoLogger;
    private final Logger warnLogger;
    private final Logger errorLogger;

    /**
     * Start creating a {@link FormattedLog} with UTC timezone for datestamps in the log
     *
     * @return a builder for a {@link FormattedLog}
     */
    public static Builder withUTCTimeZone()
    {
        return new Builder().withUTCZoneId();
    }

    /**
     * Start creating a {@link FormattedLog} with the specified zoneId from timezone for datestamps in the log
     *
     * @return a builder for a {@link FormattedLog}
     * @param timezone
     * @deprecated use {@link #withZoneId(ZoneId)}
     */
    @Deprecated
    public static Builder withTimeZone( TimeZone timezone )
    {
        return new Builder().withZoneId( timezone.toZoneId() );
    }

    /**
     * Start creating a {@link FormattedLog} with the specified zoneId for datestamps in the log
     *
     * @param zoneId
     * @return a builder for a {@link FormattedLog}
     */
    public static Builder withZoneId( ZoneId zoneId )
    {
        return new Builder().withZoneId( zoneId );
    }

    /**
     * Start creating a {@link FormattedLog} using the specified object to synchronize on.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @param lock the object to synchronize on
     * @return a builder for a {@link FormattedLog}
     */
    public static Builder usingLock( Object lock )
    {
        return new Builder().usingLock( lock );
    }

    /**
     * Include the specified category in each output log line
     *
     * @param category the category to include ing each output line
     * @return a builder for a {@link FormattedLog}
     */
    public static Builder withCategory( String category )
    {
        return new Builder().withCategory( category );
    }

    /**
     * Start creating a {@link FormattedLog} with the specified log {@link Level} as a default.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @param level the log level to use as a default
     * @return a builder for a {@link FormattedLog}
     */
    public static Builder withLogLevel( Level level )
    {
        return new Builder().withLogLevel( level );
    }

    /**
     * Start creating a {@link FormattedLog} without auto flushing.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @return a builder for a {@link FormattedLog}
     */
    public static Builder withoutAutoFlush()
    {
        return new Builder().withoutAutoFlush();
    }

    /**
     * Creates a {@link FormattedLog} instance that writes messages to an {@link OutputStream}.
     *
     * @param out An {@link OutputStream} to write to
     * @return A {@link FormattedLog} instance that writes to the specified OutputStream
     */
    public static FormattedLog toOutputStream( OutputStream out )
    {
        return new Builder().toOutputStream( out );
    }

    /**
     * Creates a {@link FormattedLog} instance that writes messages to {@link OutputStream}s obtained from the specified
     * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
     *
     * @param outSupplier A supplier for an output stream to write to
     * @return A {@link FormattedLog} instance
     */
    public static FormattedLog toOutputStream( Supplier<OutputStream> outSupplier )
    {
        return new Builder().toOutputStream( outSupplier );
    }

    /**
     * Creates a {@link FormattedLog} instance that writes messages to a {@link Writer}.
     *
     * @param writer A {@link Writer} to write to
     * @return A {@link FormattedLog} instance that writes to the specified Writer
     */
    public static FormattedLog toWriter( Writer writer )
    {
        return new Builder().toWriter( writer );
    }

    /**
     * Creates a {@link FormattedLog} instance that writes messages to a {@link PrintWriter}.
     *
     * @param writer A {@link PrintWriter} to write to
     * @return A {@link FormattedLog} instance that writes to the specified PrintWriter
     */
    public static FormattedLog toPrintWriter( PrintWriter writer )
    {
        return new Builder().toPrintWriter( writer );
    }

    /**
     * Creates a {@link FormattedLog} instance that writes messages to {@link PrintWriter}s obtained from the specified
     * {@link Supplier}. The PrintWriter is obtained from the Supplier before every log message is written.
     *
     * @param writerSupplier A supplier for a {@link PrintWriter} to write to
     * @return A {@link FormattedLog} instance that writes to the specified PrintWriter
     */
    public static FormattedLog toPrintWriter( Supplier<PrintWriter> writerSupplier )
    {
        return new Builder().toPrintWriter( writerSupplier );
    }

    protected FormattedLog(
            Supplier<PrintWriter> writerSupplier,
            ZoneId zoneId,
            Object maybeLock,
            String category,
            Level level,
            boolean autoFlush )
    {
        this( writerSupplier, zoneId, maybeLock, category, level, autoFlush,
                FormattedLogger.DATE_TIME_FORMATTER,
                () -> FormattedLogger.DEFAULT_CURRENT_DATE_TIME.apply( zoneId ) );
    }

    protected FormattedLog(
            Supplier<PrintWriter> writerSupplier,
            ZoneId zoneId,
            Object maybeLock,
            String category,
            Level level,
            boolean autoFlush,
            DateTimeFormatter dateTimeFormatter,
            Supplier<ZonedDateTime> dateTimeSupplier )
    {
        this.writerSupplier = writerSupplier;
        this.zoneId = zoneId;
        this.lock = ( maybeLock != null ) ? maybeLock : this;
        this.category = category;
        this.levelRef = new AtomicReference<>( level );
        this.autoFlush = autoFlush;

        String debugPrefix = ( category != null && !category.isEmpty() ) ? "DEBUG [" + category + "]" : "DEBUG";
        String infoPrefix = ( category != null && !category.isEmpty() ) ? "INFO [" + category + "]" : "INFO ";
        String warnPrefix = ( category != null && !category.isEmpty() ) ? "WARN [" + category + "]" : "WARN ";
        String errorPrefix = ( category != null && !category.isEmpty() ) ? "ERROR [" + category + "]" : "ERROR";

        this.debugLogger = new FormattedLogger( this, writerSupplier, debugPrefix, dateTimeFormatter,
                dateTimeSupplier );
        this.infoLogger = new FormattedLogger( this, writerSupplier, infoPrefix, dateTimeFormatter,
                dateTimeSupplier );
        this.warnLogger = new FormattedLogger( this, writerSupplier, warnPrefix, dateTimeFormatter,
                dateTimeSupplier );
        this.errorLogger = new FormattedLogger( this, writerSupplier, errorPrefix, dateTimeFormatter,
                dateTimeSupplier );
    }

    /**
     * Get the current {@link Level} that logging is enabled at
     *
     * @return the current level that logging is enabled at
     */
    public Level getLevel()
    {
        return levelRef.get();
    }

    /**
     * Set the {@link Level} that logging should be enabled at
     *
     * @param level the new logging level
     * @return the previous logging level
     */
    public Level setLevel( Level level )
    {
        return levelRef.getAndSet( level );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return Level.DEBUG.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger debugLogger()
    {
        return isDebugEnabled() ? this.debugLogger : NullLogger.getInstance();
    }

    /**
     * @return true if the current log level enables info logging
     */
    public boolean isInfoEnabled()
    {
        return Level.INFO.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger infoLogger()
    {
        return isInfoEnabled() ? this.infoLogger : NullLogger.getInstance();
    }

    /**
     * @return true if the current log level enables warn logging
     */
    public boolean isWarnEnabled()
    {
        return Level.WARN.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger warnLogger()
    {
        return isWarnEnabled() ? this.warnLogger : NullLogger.getInstance();
    }

    /**
     * @return true if the current log level enables error logging
     */
    public boolean isErrorEnabled()
    {
        return Level.ERROR.compareTo( levelRef.get() ) >= 0;
    }

    @Nonnull
    @Override
    public Logger errorLogger()
    {
        return isErrorEnabled() ? this.errorLogger : NullLogger.getInstance();
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        PrintWriter writer;
        synchronized ( lock )
        {
            writer = writerSupplier.get();
            consumer.accept( new FormattedLog( Suppliers.singleton( writer ), zoneId,
                    lock, category, levelRef.get(), false ) );
        }
        if ( autoFlush )
        {
            writer.flush();
        }
    }

}
