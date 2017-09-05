/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.query;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.logging.Log;

class QueryLogger implements QueryExecutionMonitor
{
    private final Log log;
    private final BooleanSupplier queryLogEnabled;
    private final LongSupplier thresholdMillis;
    private final boolean logQueryParameters;
    private final boolean logDetailedTime;
    private final boolean logAllocatedBytes;
    private final boolean logPageDetails;
    private final EnumSet<QueryLogEntryContent> flags;

    private static final Pattern PASSWORD_PATTERN = Pattern.compile(
            // call signature
            "(?:(?i)call)\\s+dbms(?:\\.security)?\\.change(?:User)?Password\\(" +
            // optional username parameter, in single, double quotes, or parametrized
            "(?:\\s*(?:'(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|[^,]*)\\s*,)?" +
            // password parameter, in single, double quotes, or parametrized
            "\\s*('(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|\\$\\w*|\\{\\w*\\})\\s*\\)" );

    QueryLogger( Log log, BooleanSupplier queryLogEnabled, LongSupplier thresholdMillis, EnumSet<QueryLogEntryContent> flags )
    {
        this.log = log;
        this.queryLogEnabled = queryLogEnabled;
        this.thresholdMillis = thresholdMillis;
        this.logQueryParameters = flags.contains( QueryLogEntryContent.LOG_PARAMETERS );
        this.logDetailedTime = flags.contains( QueryLogEntryContent.LOG_DETAILED_TIME );
        this.logAllocatedBytes = flags.contains( QueryLogEntryContent.LOG_ALLOCATED_BYTES );
        this.logPageDetails = flags.contains( QueryLogEntryContent.LOG_PAGE_DETAILS );
        this.flags = flags;
    }

    @Override
    public void startQueryExecution( ExecutingQuery query )
    {
    }

    @Override
    public void endFailure( ExecutingQuery query, Throwable failure )
    {
        if ( queryLogEnabled.getAsBoolean() )
        {
            log.error( logEntry( query.snapshot() ), failure );
        }
    }

    @Override
    public void endSuccess( ExecutingQuery query )
    {
        if ( queryLogEnabled.getAsBoolean() )
        {
            QuerySnapshot snapshot = query.snapshot();
            if ( snapshot.elapsedTimeMillis() >= thresholdMillis.getAsLong() )
            {
                log.info( logEntry( snapshot ) );
            }
        }
    }

    private String logEntry( QuerySnapshot query )
    {
        String sourceString = query.clientConnection().asConnectionDetails();
        String queryText = query.queryText();

        Set<String> passwordParams = new HashSet<>();
        Matcher matcher = PASSWORD_PATTERN.matcher( queryText );

        while ( matcher.find() )
        {
            String password = matcher.group( 1 ).trim();
            if ( password.charAt( 0 ) == '$' )
            {
                passwordParams.add( password.substring( 1 ) );
            }
            else if ( password.charAt( 0 ) == '{' )
            {
                passwordParams.add( password.substring( 1, password.length() - 1 ) );
            }
            else
            {
                queryText = queryText.replace( password, "******" );
            }
        }

        StringBuilder result = new StringBuilder();
        result.append( query.elapsedTimeMillis() ).append( " ms: " );
        if ( logDetailedTime )
        {
            QueryLogFormatter.formatDetailedTime( result, query );
        }
        if ( logAllocatedBytes )
        {
            QueryLogFormatter.formatAllocatedBytes( result, query );
        }
        if ( logPageDetails )
        {
            QueryLogFormatter.formatPageDetails( result, query );
        }
        result.append( sourceString ).append( " - " ).append( queryText );
        if ( logQueryParameters )
        {
            QueryLogFormatter.formatMapValue( result.append(" - "), query.queryParameters(), passwordParams );
        }
        QueryLogFormatter.formatMap( result.append(" - "), query.transactionAnnotationData() );
        return result.toString();
    }
}
