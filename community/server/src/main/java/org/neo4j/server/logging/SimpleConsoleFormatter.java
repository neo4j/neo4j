/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public class SimpleConsoleFormatter extends Formatter
{
    private Date date = new Date();
    private final static String timestampFormat = "{0,date,short} {0,time}";
    private final MessageFormat formatter = new MessageFormat( timestampFormat );

    private final Object args[] = { date };

    private static final String lineSeparator = System.getProperty( "line.separator" );

    /**
     * Format the given LogRecord.
     * 
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public synchronized String format( LogRecord record )
    {
        StringBuffer sb = new StringBuffer();

        // Do the timestamp formatting
        date.setTime( record.getMillis() );
        StringBuffer text = new StringBuffer();
        formatter.format( args, text, null );

        sb.append( text );

        /*
         * Since we are using our own class over the logger,
         * the source class/method names are useless. If, however,
         * everyone plays nice and names properly their logger, then
         * the following should provide enough info.
         */
        sb.append( " " );
        sb.append( record.getLoggerName() );
        sb.append( " " );
        String message = formatMessage( record );
        sb.append( record.getLevel()
                .getLocalizedName() );
        sb.append( ": " );
        sb.append( message );
        sb.append( lineSeparator );
        if ( record.getThrown() != null )
        {
            try
            {
                StringWriter sw = new StringWriter();
                PrintWriter pw = new PrintWriter( sw );
                record.getThrown()
                        .printStackTrace( pw );
                pw.close();
                sb.append( sw.toString() );
            }
            catch ( Exception ex )
            {
            }
        }
        return sb.toString();
    }

}
