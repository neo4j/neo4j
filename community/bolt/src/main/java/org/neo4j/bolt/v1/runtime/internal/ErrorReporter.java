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
package org.neo4j.bolt.v1.runtime.internal;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.udc.UsageData;
import org.neo4j.udc.UsageDataKeys;

import static javax.xml.bind.DatatypeConverter.printBase64Binary;

/**
 * Convert the mixed exceptions the underlying engine can throw to a cohesive set of known failures. This is an
 * intermediary mechanism.
 */
public class ErrorReporter
{
    private static final String NO_ERROR_DATA_AVAILABLE = "(No error data available)";

    private final Log userLog;
    private final UsageData usageData;

    public ErrorReporter( LogService logging, UsageData usageData )
    {
        this(logging.getUserLog( ErrorReporter.class ), usageData );
    }

    public ErrorReporter( Log userLog, UsageData usageData )
    {
        this.userLog = userLog;
        this.usageData = usageData;
    }

    public void report( Neo4jError error )
    {
        if ( !error.status().code().classification().publishable() )
        {
            // Log unknown errors.
            userLog.error( String.format(
                    "Client triggered unexpected error. Help us fix this error by emailing the " +
                            "following report to issues@neotechnology.com: %n%s",
                    generateReport( error ) ) );
        }
    }

    /**
     * For unrecognized errors, we generate a report and urge users to send it to us for debugging. The report just contains some basic system info
     * and a base64-encoded stack trace.
     */
    private String generateReport( Neo4jError error )
    {
        // To avoid having users send us just the exception message or just part of the stack traces, encode the whole
        // stack trace into a Base64 string, and wrap the report explicitly in start/end markers to show users where
        // to cut and paste.
        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mmZ");
        dateFormat.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        return String.format( "---- START OF REPORT ----%n" +
                              "Date             : %s%n" +
                              "Neo4j Version    : %s %s%n" +
                              "Operational mode : %s%n" +
                              "Java Version     : %s%n" +
                              "OS               : %s%n" +
                              "Reference        : %s%n" + // This will help us correlate emailed reports with user logs
                              "Status code      : %s&n" +
                              "Message          : %s&n" +
                              "Error data       : %n" +
                              "%s%n" +
                              "---- END OF REPORT ----%n",
                dateFormat.format( new Date() ),
                usageData.get( UsageDataKeys.edition ).name(),
                usageData.get( UsageDataKeys.version ),
                usageData.get( UsageDataKeys.operationalMode ).name(),
                System.getProperty("java.version"),
                System.getProperty("os.name"),
                error.reference(),
                error.status(),
                error.message(),
                errorData( error.cause() ) );
    }

    public String errorData( Throwable cause )
    {
        if ( cause == null )
        {
            return NO_ERROR_DATA_AVAILABLE;
        }
        else
        {
            return printBase64Binary(
                    Exceptions.stringify( cause ).getBytes( StandardCharsets.UTF_8 ) )
                    // Below replaceAll call inserts a line break every 100 characters
                    .replaceAll( "(.{100})", "$1" + System.lineSeparator() );
        }
    }

}
