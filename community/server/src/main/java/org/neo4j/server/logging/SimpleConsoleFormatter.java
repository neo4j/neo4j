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
package org.neo4j.server.logging;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import org.neo4j.kernel.logging.ModuleConverter;

import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.CoreConstants;

public class SimpleConsoleFormatter extends Formatter
{
    private final PatternLayoutEncoder encoder;

    public SimpleConsoleFormatter()
    {
        LoggerContext context = new LoggerContext();

        Map<String, String> converters = new HashMap<String, String>();
        converters.put("module", ModuleConverter.class.getName());
        context.putObject(CoreConstants.PATTERN_RULE_REGISTRY, converters);
        encoder = new PatternLayoutEncoder();
        encoder.setContext(context);
        encoder.setPattern("%date{yyyy-MM-dd HH:mm:ss.SSSZ,UTC} %-5level [%module] %message%n");
        encoder.start();
    }

    /**
     * Format the given LogRecord.
     *
     * @param record the log record to be formatted.
     * @return a formatted log record
     */
    @Override
    public synchronized String format( LogRecord record )
    {
        ILoggingEvent event = new LoggingEvent(record.getLoggerName(), (Logger) LoggerFactory.getLogger(record.getLoggerName()), Level.toLevel(record.getLevel().getName()), record.getMessage(), record.getThrown(), record.getParameters());
        return encoder.getLayout().doLayout(event);

/*        StringBuffer sb = new StringBuffer();

        // Do the timestamp formatting
        date.setTime( record.getMillis() );
        StringBuffer text = new StringBuffer();
        formatter.format( args, text, null );

        sb.append( text );

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
        return sb.toString();*/
    }

}
