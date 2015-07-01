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
package org.neo4j.ndp.runtime.internal;

import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static javax.xml.bind.DatatypeConverter.printBase64Binary;

/**
 * Convert the mixed exceptions the underlying engine can throw to a cohesive set of known failures. This is an
 * intermediary mechanism.
 */
public class ErrorTranslator
{
    private final Log userLog;

    public ErrorTranslator( LogService logging )
    {
        this(logging.getUserLog( ErrorTranslator.class ) );
    }

    public ErrorTranslator( Log userLog )
    {
        this.userLog = userLog;
    }

    public Neo4jError translate( Throwable any )
    {
        for( Throwable cause = any; cause != null; cause = cause.getCause() )
        {
            if ( cause instanceof Status.HasStatus )
            {
                return new Neo4jError( ((Status.HasStatus) cause).status(), any.getMessage() );
            }
        }

        // In this case, an error has "slipped out", and we don't have a good way to handle it. This indicates
        // a buggy code path, and we need to try to convince whoever ends up here to tell us about it.

        // TODO: Perhaps move this whole block to somewhere more sensible?
        String reference = UUID.randomUUID().toString();

        // Log unknown errors.
        userLog.error( String.format(
                "Client triggered unexpected error. Help us fix this error by emailing the " +
                "following report to issues@neotechnology.com: %n%s", generateReport( any, reference ) ) );

        return new Neo4jError( Status.General.UnknownFailure,
                String.format( "An unexpected failure occurred, see details in the database " +
                               "logs, reference number %s.%n%s", reference, Exceptions.stringify( any ) ) );
    }

    /**
     * For unrecognized errors, we generate a report and urge users to send it to us for debugging. The report just contains some basic system info
     * and a base64-encoded stack trace.
     */
    private String generateReport( Throwable err, String reference )
    {
        // To avoid having users send us just the exception message or just part of the stack traces, encode the whole
        // stack trace into a Base64 string, and wrap the report explicitly in start/end markers to show users where
        // to cut and paste.
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyy-MMM-dd HH:mm:ss");
        dateFormatGmt.setTimeZone( TimeZone.getTimeZone( "GMT" ) );

        return String.format( "---- START OF REPORT ----%n" +
                              "Date          : %s GMT%n" +
                              "Neo4j Version : %s%n" +
                              "Java Version  : %s%n" +
                              "OS            : %s%n" +
                              "Reference     : %s%n" + // This will help us correlate emailed reports with user logs
                              "Error data    : %n" +
                              "%s" +
                              "---- END OF REPORT ----%n",
                dateFormatGmt.format( new Date() ),
                Version.getKernelVersion(),
                System.getProperty("java.version"),
                System.getProperty("os.name"),
                reference,
                printBase64Binary( Exceptions.stringify( err ).getBytes( StandardCharsets.UTF_8 ) )
                        // Below replaceAll call inserts a line break every 100 characters
                        .replaceAll( "(.{100})", "$1" + System.getProperty("line.separator") ) );
    }

}
