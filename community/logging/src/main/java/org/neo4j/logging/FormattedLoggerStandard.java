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
package org.neo4j.logging;

import java.io.PrintWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import org.neo4j.function.Suppliers;

class FormattedLoggerStandard extends AbstractPrintWriterLogger
{
    static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss.SSSZ" );
    static final Function<ZoneId, ZonedDateTime> DEFAULT_CURRENT_DATE_TIME = zoneId ->
            ZonedDateTime.now()
                    .withZoneSameInstant( zoneId );
    private final String category;
    private final Level level;
    private FormattedLog formattedLog;
    private final String prefix;
    private final DateTimeFormatter dateTimeFormatter;
    private Supplier<ZonedDateTime> supplier;

    FormattedLoggerStandard( FormattedLog formattedLog, @Nonnull Supplier<PrintWriter> writerSupplier,
                             @Nonnull Level level, String category, DateTimeFormatter dateTimeFormatter,
                             Supplier<ZonedDateTime> zonedDateTimeSupplier )
    {
        super( writerSupplier, formattedLog.lock, formattedLog.autoFlush );

        this.formattedLog = formattedLog;
        this.level = level;
        this.category = category;
        this.prefix = createPrefix( level, category );
        this.dateTimeFormatter = dateTimeFormatter;
        this.supplier = zonedDateTimeSupplier;
    }

    private static String createPrefix( Level level, String category )
    {
        switch ( level )
        {
        case DEBUG:
            return (category != null && !category.isEmpty()) ? "DEBUG [" + category + "]" : "DEBUG";
        case INFO:
            return (category != null && !category.isEmpty()) ? "INFO [" + category + "]" : "INFO ";
        case WARN:
            return (category != null && !category.isEmpty()) ? "WARN [" + category + "]" : "WARN ";
        case ERROR:
            return (category != null && !category.isEmpty()) ? "ERROR [" + category + "]" : "ERROR";
        default:
            throw new IllegalArgumentException( "Cannot create FormattedLogger with Level " + level );
        }
    }

    @Override
    protected void writeLog( @Nonnull PrintWriter out, @Nonnull String message )
    {
        lineStart( out );
        out.write( message );
        out.println();
    }

    @Override
    protected void writeLog( @Nonnull PrintWriter out, @Nonnull String message, @Nonnull Throwable throwable )
    {
        lineStart( out );
        out.write( message );
        if ( throwable.getMessage() != null )
        {
            out.write( ' ' );
            out.write( throwable.getMessage() );
        }
        out.println();
        throwable.printStackTrace( out );
    }

    @Override
    protected Logger getBulkLogger( @Nonnull PrintWriter out, @Nonnull Object lock )
    {
        return new FormattedLoggerStandard( formattedLog, Suppliers.singleton( out ), level, category, DATE_TIME_FORMATTER,
                () -> DEFAULT_CURRENT_DATE_TIME.apply( formattedLog.zoneId ) );
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
        return dateTimeFormatter.format( supplier.get() );
    }
}
