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
package org.neo4j.kernel.enterprise.builtinprocs;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;

public class QueryStatusResult
{
    public final String queryId;
    public final String username;
    public final String query;
    public final Map<String,Object> parameters;
    public final String planner;
    public final String runtime;
    public final String startTime;
    @Deprecated
    public final String elapsedTime;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final long elapsedTimeMillis; // TODO: this field should be of a Duration type (when Cypher supports that)
    @Deprecated
    public final String connectionDetails;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final String requestScheme;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final String clientAddress;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final String requestUri;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final long cpuTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final Map<String,Object> status;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final long activeLockCount;
    /** EXPERIMENTAL: added in Neo4j 3.2 */
    public final long waitTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    public final Map<String,Object> metaData;
    public final List<Map<String,String>> indexes;

    QueryStatusResult( ExecutingQuery q ) throws InvalidArgumentsException
    {
        this(
                ofInternalId( q.internalQueryId() ),
                q.username(),
                q.query(),
                q.startTime(),
                q.elapsedTimeMillis(),
                q.clientConnection(),
                q.metaData(),
                q.cpuTimeMillis(),
                q.status(),
                q.activeLockCount(),
                q.waitTimeMillis() );
    }

    private QueryStatusResult(
            QueryId queryId,
            String username,
            ExecutingQuery.QueryInfo query,
            long startTime,
            long elapsedTime,
            ClientConnectionInfo clientConnection,
            Map<String,Object> txMetaData,
            long cpuTimeMillis,
            Map<String,Object> status,
            long activeLockCount,
            long waitTimeMillis
    ) {
        this.queryId = queryId.toString();
        this.username = username;
        this.query = query.text;
        this.parameters = query.parameters;
        this.startTime = formatTime( startTime );
        this.elapsedTime = formatInterval( elapsedTime );
        this.elapsedTimeMillis = elapsedTime;
        this.connectionDetails = clientConnection.asConnectionDetails();
        this.requestScheme = clientConnection.requestScheme();
        this.clientAddress = clientConnection.clientAddress();
        this.requestUri = clientConnection.requestURI();
        this.metaData = txMetaData;
        this.cpuTimeMillis = cpuTimeMillis;
        this.status = status;
        this.activeLockCount = activeLockCount;
        this.waitTimeMillis = waitTimeMillis;
        this.planner = query.planner;
        this.runtime = query.runtime;
        this.indexes = query.indexes();
    }

    private static String formatTime( final long startTime )
    {
        return OffsetDateTime
            .ofInstant( Instant.ofEpochMilli( startTime ), ZoneId.systemDefault() )
            .format( ISO_OFFSET_DATE_TIME );
    }

    private static String formatInterval( final long l )
    {
        final long hr = MILLISECONDS.toHours( l );
        final long min = MILLISECONDS.toMinutes( l - HOURS.toMillis( hr ) );
        final long sec = MILLISECONDS.toSeconds( l - HOURS.toMillis( hr ) - MINUTES.toMillis( min ) );
        final long ms = l - HOURS.toMillis( hr ) - MINUTES.toMillis( min ) - SECONDS.toMillis( sec );
        return String.format( "%02d:%02d:%02d.%03d", hr, min, sec, ms );
    }
}
