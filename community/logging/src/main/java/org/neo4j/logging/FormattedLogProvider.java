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
package org.neo4j.logging;

import org.neo4j.function.Supplier;
import org.neo4j.function.Suppliers;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
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
        private Map<String, Level> levels = new HashMap<>();
        private Level defaultLevel = Level.INFO;
        private boolean autoFlush = true;

        private Builder()
        {
        }

        /**
         * Disable rendering of the context (the class name or log name) in each output line.
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
        public Builder withDefaultLogLevel( Level level )
        {
            this.defaultLevel = level;
            return this;
        }

        /**
         * Use the specified log {@link Level} for any {@link Log}s that match the specified context. Any {@link Log} context that
         * starts with the specified string will have its level set. For example, setting the level for the context {@code org.neo4j}
         * would result in that level being applied to {@link Log}s with the context {@code org.neo4j.Foo}, {@code org.neo4j.foo.Bar}, etc.
         *
         * @param context the context of the Logs to set the level of, matching any Log context starting with this string
         * @param level the log level to apply
         * @return this builder
         */
        public Builder withLogLevel( String context, Level level )
        {
            this.levels.put( context, level );
            return this;
        }

        /**
         * Set the log level for many contexts - equivalent to calling {@link #withLogLevel(String, Level)} for every entry in the provided map.
         *
         * @param levels a map containing paris of context and level
         */
        public Builder withLogLevels( Map<String, Level> levels )
        {
            this.levels.putAll( levels );
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
            return new FormattedLogProvider( DEFAULT_CURRENT_DATE_SUPPLIER, writerSupplier, timezone, renderContext, levels, defaultLevel, autoFlush );
        }
    }

    private final Supplier<Date> currentDateSupplier;
    private final Supplier<PrintWriter> writerSupplier;
    private final TimeZone timezone;
    private final boolean renderContext;
    private final Map<String, Level> levels;
    private final Level defaultLevel;
    private final boolean autoFlush;

    /**
     * Start creating a {@link FormattedLogProvider} which will not render the context (the class name or log name) in each output line.
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
    public static Builder withDefaultLogLevel( Level level )
    {
        return new Builder().withDefaultLogLevel( level );
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

    FormattedLogProvider( Supplier<Date> currentDateSupplier, Supplier<PrintWriter> writerSupplier, TimeZone timezone,boolean renderContext,
            Map<String, Level> levels, Level defaultLevel, boolean autoFlush )
    {
        this.currentDateSupplier = currentDateSupplier;
        this.writerSupplier = writerSupplier;
        this.timezone = timezone;
        this.renderContext = renderContext;
        this.levels = new HashMap<>( levels );
        this.defaultLevel = defaultLevel;
        this.autoFlush = autoFlush;
    }

    @Override
    protected FormattedLog buildLog( Class loggingClass )
    {
        String className = loggingClass.getName();
        String shortenedClassName = PACKAGE_PATTERN.matcher( className ).replaceAll( "$1." );
        return buildLog( shortenedClassName, levelForContext( className ) );
    }

    @Override
    protected FormattedLog buildLog( String name )
    {
        return buildLog( name, levelForContext( name ) );
    }

    private FormattedLog buildLog( String context, Level level )
    {
        return new FormattedLog( currentDateSupplier, writerSupplier, timezone, this, renderContext ? context : null, level, autoFlush );
    }

    private Level levelForContext( String context )
    {
        for ( Map.Entry<String, Level> entry : levels.entrySet() )
        {
            if ( context.startsWith( entry.getKey() ) )
            {
                return entry.getValue();
            }
        }
        return defaultLevel;
    }
}
