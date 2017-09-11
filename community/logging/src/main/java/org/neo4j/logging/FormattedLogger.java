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

import java.io.PrintWriter;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import org.neo4j.function.Suppliers;

class FormattedLogger extends AbstractPrintWriterLogger
{
    static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern( "yyyy-MM-dd HH:mm:ss.SSSZ" );
    static final Function<ZoneId, ZonedDateTime> DEFAULT_CURRENT_DATE_TIME = zoneId ->
            ZonedDateTime.now()
                    .withZoneSameInstant( zoneId );
    private FormattedLog formattedLog;
    private final String prefix;
    private final DateTimeFormatter dateTimeFormatter;
    private Supplier<ZonedDateTime> supplier;

    FormattedLogger( FormattedLog formattedLog, @Nonnull Supplier<PrintWriter> writerSupplier,
                     @Nonnull String prefix, DateTimeFormatter dateTimeFormatter,
                     Supplier<ZonedDateTime> zonedDateTimeSupplier )
    {
        super( writerSupplier, formattedLog.lock, formattedLog.autoFlush );

        this.formattedLog = formattedLog;
        this.prefix = prefix;
        this.dateTimeFormatter = dateTimeFormatter;
        this.supplier = zonedDateTimeSupplier;
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
        return new FormattedLogger( formattedLog, Suppliers.singleton( out ), prefix, DATE_TIME_FORMATTER,
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
