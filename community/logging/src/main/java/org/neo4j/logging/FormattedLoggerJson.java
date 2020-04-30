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

import org.codehaus.jettison.json.JSONObject;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import org.neo4j.function.Suppliers;

import static java.lang.String.format;
import static org.neo4j.logging.FormattedLoggerStandard.DATE_TIME_FORMATTER;
import static org.neo4j.logging.FormattedLoggerStandard.DEFAULT_CURRENT_DATE_TIME;

class FormattedLoggerJson extends AbstractPrintWriterLogger
{
    private final String category;
    private final Level level;
    private final String levelCategoryString;
    private final DateTimeFormatter dateTimeFormatter;
    private final FormattedLog formattedLog;
    private final Supplier<ZonedDateTime> supplier;

    FormattedLoggerJson( FormattedLog formattedLog, @Nonnull Supplier<PrintWriter> writerSupplier,
                         @Nonnull Level level, String category, DateTimeFormatter dateTimeFormatter,
                         Supplier<ZonedDateTime> zonedDateTimeSupplier )
    {
        super( writerSupplier, formattedLog.lock, formattedLog.autoFlush );

        validateLevel( level );

        this.formattedLog = formattedLog;
        this.level = level;
        this.category = category;
        this.levelCategoryString = createLevelCategoryString( level, category );
        this.dateTimeFormatter = dateTimeFormatter;
        this.supplier = zonedDateTimeSupplier;
    }

    private static String createLevelCategoryString( Level level, String category )
    {
        if ( category != null && !category.isEmpty() )
        {
            return format( "\"level\": \"%s\", \"category\": %s", level.name(), JSONObject.quote( category ) );
        }
        else
        {
            return format( "\"level\": \"%s\"", level.name() );
        }
    }

    private static void validateLevel( Level level )
    {
        switch ( level )
        {
        case DEBUG:
        case INFO:
        case WARN:
        case ERROR:
            return;
        default:
            throw new IllegalArgumentException( "Cannot create JsonFormattedLogger with Level " + level );
        }
    }

    @Override
    protected void writeLog( @Nonnull PrintWriter out, @Nonnull String message )
    {
        lineStart( out );
        out.write( JSONObject.quote( message ) );
        lineEnd( out );
        out.println();
    }

    @Override
    protected void writeLog( @Nonnull PrintWriter out, @Nonnull String message, @Nonnull Throwable throwable )
    {
        lineStart( out );
        out.write( JSONObject.quote( message ) );
        if ( throwable.getMessage() != null )
        {
            out.write( ", \"stacktraceMessage\": " );
            out.write( JSONObject.quote( throwable.getMessage() ) );
        }
        out.write( ", \"stacktrace\": " );
        StringWriter sw = new StringWriter();
        PrintWriter stackTraceWriter = new PrintWriter( sw );
        throwable.printStackTrace( stackTraceWriter );
        out.write( JSONObject.quote( sw.toString() ) );
        lineEnd( out );
    }

    @Override
    protected Logger getBulkLogger( @Nonnull PrintWriter out, @Nonnull Object lock )
    {
        return new FormattedLoggerJson( formattedLog, Suppliers.singleton( out ), level, category,
                                        DATE_TIME_FORMATTER, () -> DEFAULT_CURRENT_DATE_TIME.apply( formattedLog.zoneId ) );
    }

    private void lineStart( PrintWriter out )
    {
        out.write( "{\"time\": \"" );
        out.write( time() );
        out.write( "\", " );
        out.write( levelCategoryString );
        out.write( ", \"message\": " );
    }

    private void lineEnd( PrintWriter out )
    {
        out.write( "}" );
    }

    private String time()
    {
        return dateTimeFormatter.format( supplier.get() );
    }
}
