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
import java.io.Writer;
import java.util.Date;
import java.util.TimeZone;
import java.util.regex.Pattern;

import static org.neo4j.logging.FormattedLog.DEFAULT_CURRENT_DATE_SUPPLIER;
import static org.neo4j.logging.FormattedLog.OUTPUT_STREAM_CONVERTER;

/**
 * A {@link LogProvider} implementation that applies a simple formatting to each log message.
 */
public class FormattedLogProvider extends AbstractLogProvider<FormattedLog>
{
    private static final Pattern PACKAGE_PATTERN = Pattern.compile( "(\\w)\\w+\\." );

    /**
     * A Builder for a {@link FormattedLogProvider}
     */
    public static class Builder
    {
        private boolean renderContext = true;
        private TimeZone timezone = TimeZone.getDefault();
        private Level level = Level.INFO;
        private boolean autoFlush = true;

        private Builder()
        {
        }

        /**
         * Disable rendering of the context (or class name) in each output line.
         *
         * @return this builder
         */
        public Builder withoutRenderingContext()
        {
            this.renderContext = false;
            return this;
        }

        /**
         * Set the timezone for datestamps in the log
         *
         * @return this builder
         */
        public Builder withUTCTimeZone()
        {
            return withTimeZone( FormattedLog.UTC );
        }

        /**
         * Set the timezone for datestamps in the log
         *
         * @param timezone the timezone to use for datestamps
         * @return this builder
         */
        public Builder withTimeZone( TimeZone timezone )
        {
            this.timezone = timezone;
            return this;
        }

        /**
         * Use the specified log {@link Level} for all {@link Log}s by default.
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
         * Disable auto flushing.
         *
         * @return this builder
         */
        public Builder withoutAutoFlush()
        {
            this.autoFlush = false;
            return this;
        }

        /**
         * Creates a {@link FormattedLogProvider} instance that writes messages to an {@link OutputStream}.
         *
         * @param out An {@link OutputStream} to write to
         * @return A {@link FormattedLogProvider} instance that writes to the specified OutputStream
         */
        public FormattedLogProvider toOutputStream( OutputStream out )
        {
            return toPrintWriter( Suppliers.singleton( OUTPUT_STREAM_CONVERTER.apply( out ) ) );
        }

        /**
         * Creates a {@link FormattedLogProvider} instance that writes messages to {@link OutputStream}s obtained from the specified
         * {@link Supplier}. The OutputStream is obtained from the Supplier before every log message is written.
         *
         * @param outSupplier A supplier for an output stream to write to
         * @return A {@link FormattedLogProvider} instance
         */
        public FormattedLogProvider toOutputStream( Supplier<OutputStream> outSupplier )
        {
            return toPrintWriter( Suppliers.adapted( outSupplier, OUTPUT_STREAM_CONVERTER ) );
        }

        /**
         * Creates a {@link FormattedLogProvider} instance that writes messages to a {@link Writer}.
         *
         * @param writer A {@link Writer} to write to
         * @return A {@link FormattedLogProvider} instance that writes to the specified Writer
         */
        public FormattedLogProvider toWriter( Writer writer )
        {
            return toPrintWriter( new PrintWriter( writer ) );
        }

        /**
         * Creates a {@link FormattedLogProvider} instance that writes messages to a {@link PrintWriter}.
         *
         * @param writer A {@link PrintWriter} to write to
         * @return A {@link FormattedLogProvider} instance that writes to the specified PrintWriter
         */
        public FormattedLogProvider toPrintWriter( PrintWriter writer )
        {
            return toPrintWriter( Suppliers.singleton( writer ) );
        }

        /**
         * Creates a {@link FormattedLogProvider} instance that writes messages to {@link PrintWriter}s obtained from the specified
         * {@link Supplier}. The PrintWriter is obtained from the Supplier before every log message is written.
         *
         * @param writerSupplier A supplier for a {@link PrintWriter} to write to
         * @return A {@link FormattedLogProvider} instance that writes to the specified PrintWriter
         */
        public FormattedLogProvider toPrintWriter( Supplier<PrintWriter> writerSupplier )
        {
            return new FormattedLogProvider( DEFAULT_CURRENT_DATE_SUPPLIER, writerSupplier, timezone, renderContext, level, autoFlush );
        }
    }

    private final Supplier<Date> currentDateSupplier;
    private final Supplier<PrintWriter> writerSupplier;
    private final TimeZone timezone;
    private final boolean renderContext;
    private final Level level;
    private final boolean autoFlush;

    /**
     * Start creating a {@link FormattedLogProvider} which will not render the context (or class name) in each output line.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @return a builder for a {@link FormattedLogProvider}
     */
    public static Builder withoutRenderingContext()
    {
        return new Builder().withoutRenderingContext();
    }

    /**
     * Start creating a {@link FormattedLogProvider} with UTC timezone for datestamps in the log
     *
     * @return a builder for a {@link FormattedLogProvider}
     */
    public static Builder withUTCTimeZone()
    {
        return new Builder().withUTCTimeZone();
    }

    /**
     * Start creating a {@link FormattedLogProvider} with the specified timezone for datestamps in the log
     *
     * @param timezone the timezone to use for datestamps
     * @return a builder for a {@link FormattedLogProvider}
     */
    public static Builder withTimeZone( TimeZone timezone )
    {
        return new Builder().withTimeZone( timezone );
    }

    /**
     * Start creating a {@link FormattedLogProvider} with the specified log {@link Level} for all {@link Log}s by default.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @param level the log level to use as a default
     * @return a builder for a {@link FormattedLogProvider}
     */
    public static Builder withLogLevel( Level level )
    {
        return new Builder().withLogLevel( level );
    }

    /**
     * Start creating a {@link FormattedLogProvider} without auto flushing.
     * Use {@link Builder#toOutputStream} to complete.
     *
     * @return a builder for a {@link FormattedLogProvider}
     */
    public static Builder withoutAutoFlush()
    {
        return new Builder().withoutAutoFlush();
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to an {@link OutputStream}.
     *
     * @param out An {@link OutputStream} to write to
     * @return A {@link FormattedLogProvider} instance that writes to the specified OutputStream
     */
    public static FormattedLogProvider toOutputStream( OutputStream out )
    {
        return new Builder().toOutputStream( out );
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
        return new Builder().toOutputStream( outSupplier );
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to a {@link Writer}.
     *
     * @param writer A {@link Writer} to write to
     * @return A {@link FormattedLogProvider} instance that writes to the specified Writer
     */
    public static FormattedLogProvider toWriter( Writer writer )
    {
        return new Builder().toWriter( writer );
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to a {@link PrintWriter}.
     *
     * @param writer A {@link PrintWriter} to write to
     * @return A {@link FormattedLogProvider} instance that writes to the specified PrintWriter
     */
    public static FormattedLogProvider toPrintWriter( PrintWriter writer )
    {
        return new Builder().toPrintWriter( writer );
    }

    /**
     * Creates a {@link FormattedLogProvider} instance that writes messages to {@link PrintWriter}s obtained from the specified
     * {@link Supplier}. The PrintWriter is obtained from the Supplier before every log message is written.
     *
     * @param writerSupplier A supplier for a {@link PrintWriter} to write to
     * @return A {@link FormattedLogProvider} instance that writes to the specified PrintWriter
     */
    public static FormattedLogProvider toPrintWriter( Supplier<PrintWriter> writerSupplier )
    {
        return new Builder().toPrintWriter( writerSupplier );
    }

    FormattedLogProvider( Supplier<Date> currentDateSupplier, Supplier<PrintWriter> writerSupplier, TimeZone timezone, boolean renderContext, Level level, boolean autoFlush )
    {
        this.currentDateSupplier = currentDateSupplier;
        this.writerSupplier = writerSupplier;
        this.timezone = timezone;
        this.renderContext = renderContext;
        this.level = level;
        this.autoFlush = autoFlush;
    }

    @Override
    protected FormattedLog buildLog( Class loggingClass )
    {
        String shortenedClassName = PACKAGE_PATTERN.matcher( loggingClass.getName() ).replaceAll( "$1." );
        return buildLog( shortenedClassName );
    }

    @Override
    protected FormattedLog buildLog( String context )
    {
        return new FormattedLog( currentDateSupplier, writerSupplier, timezone, this, renderContext ? context : null, level, autoFlush );
    }
}
