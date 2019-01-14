/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.kernel.impl.query;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

class ConfiguredQueryLogger implements QueryLogger
{
    private final Log log;
    private final long thresholdMillis;
    private final boolean logQueryParameters;
    private final boolean logDetailedTime;
    private final boolean logAllocatedBytes;
    private final boolean logPageDetails;
    private final boolean logRuntime;

    private static final Pattern PASSWORD_PATTERN = Pattern.compile(
            // call signature
            "(?:(?i)call)\\s+dbms(?:\\.security)?\\.change(?:User)?Password\\(" +
            // optional username parameter, in single, double quotes, or parametrized
            "(?:\\s*(?:'(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|[^,]*)\\s*,)?" +
            // password parameter, in single, double quotes, or parametrized
            "\\s*('(?:(?<=\\\\)'|[^'])*'|\"(?:(?<=\\\\)\"|[^\"])*\"|\\$\\w*|\\{\\w*})\\s*\\)" );

    ConfiguredQueryLogger( Log log, Config config )
    {
        this.log = log;
        this.thresholdMillis = config.get( GraphDatabaseSettings.log_queries_threshold ).toMillis();
        this.logQueryParameters = config.get( GraphDatabaseSettings.log_queries_parameter_logging_enabled );
        this.logDetailedTime = config.get( GraphDatabaseSettings.log_queries_detailed_time_logging_enabled );
        this.logAllocatedBytes = config.get( GraphDatabaseSettings.log_queries_allocation_logging_enabled );
        this.logPageDetails = config.get( GraphDatabaseSettings.log_queries_page_detail_logging_enabled );
        this.logRuntime = config.get( GraphDatabaseSettings.log_queries_runtime_logging_enabled );
    }

    @Override
    public void failure( ExecutingQuery query, Throwable failure )
    {
        log.error( logEntry( query.snapshot() ), failure );
    }

    @Override
    public void success( ExecutingQuery query )
    {
        if ( NANOSECONDS.toMillis( query.elapsedNanos() ) >= thresholdMillis )
        {
            QuerySnapshot snapshot = query.snapshot();
            log.info( logEntry( snapshot ) );
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
        if ( logRuntime )
        {
            result.append( " - runtime=" ).append( query.runtime() );
        }
        QueryLogFormatter.formatMap( result.append(" - "), query.transactionAnnotationData() );
        return result.toString();
    }
}
