/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.kernel.enterprise.builtinprocs;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.query.clientconnection.ClientConnectionInfo;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.enterprise.builtinprocs.QueryId.ofInternalId;

public class QueryStatusResult
{
    public static final ZoneId UTC_ZONE_ID = ZoneId.of("UTC");
    public final String queryId;
    public final String username;
    public final Map<String,Object> metaData;
    public final String query;
    public final Map<String,Object> parameters;
    /** @since Neo4j 3.2 */
    public final String planner;
    /** @since Neo4j 3.2 */
    public final String runtime;
    /** @since Neo4j 3.2 */
    public final List<Map<String,String>> indexes;
    public final String startTime;
    @Deprecated
    public final String elapsedTime;
    @Deprecated
    public final String connectionDetails;
    /** @since Neo4j 3.2 */
    public final String protocol;
    /** @since Neo4j 3.2 */
    public final String clientAddress;
    /** @since Neo4j 3.2 */
    public final String requestUri;
    /** @since Neo4j 3.2 */
    public final String status;
    /** @since Neo4j 3.2 */
    public final Map<String,Object> resourceInformation;
    /** @since Neo4j 3.2 */
    public final long activeLockCount;
    /** @since Neo4j 3.2 */
    public final long elapsedTimeMillis; // TODO: this field should be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2, will be {@code null} if measuring CPU time is not supported. */
    public final Long cpuTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2 */
    public final long waitTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2 */
    public final Long idleTimeMillis; // TODO: we want this field to be of a Duration type (when Cypher supports that)
    /** @since Neo4j 3.2, will be {@code null} if measuring allocation is not supported. */
    public final Long allocatedBytes;
    /** @since Neo4j 3.2 */
    public final long pageHits;
    /** @since Neo4j 3.2 */
    public final long pageFaults;

    QueryStatusResult( ExecutingQuery query ) throws InvalidArgumentsException
    {
        this( query.snapshot() );
    }

    private QueryStatusResult( QuerySnapshot query ) throws InvalidArgumentsException
    {
        this.queryId = ofInternalId( query.internalQueryId() ).toString();
        this.username = query.username();
        this.query = query.queryText();
        this.parameters = query.queryParameters();
        this.startTime = formatTime( query.startTimestampMillis() );
        this.elapsedTimeMillis = query.elapsedTimeMillis();
        this.elapsedTime = formatInterval( elapsedTimeMillis );
        ClientConnectionInfo clientConnection = query.clientConnection();
        this.connectionDetails = clientConnection.asConnectionDetails();
        this.protocol = clientConnection.protocol();
        this.clientAddress = clientConnection.clientAddress();
        this.requestUri = clientConnection.requestURI();
        this.metaData = query.transactionAnnotationData();
        this.cpuTimeMillis = query.cpuTimeMillis();
        this.status = query.status();
        this.resourceInformation = query.resourceInformation();
        this.activeLockCount = query.activeLockCount();
        this.waitTimeMillis = query.waitTimeMillis();
        this.idleTimeMillis = query.idleTimeMillis();
        this.planner = query.planner();
        this.runtime = query.runtime();
        this.indexes = query.indexes();
        this.allocatedBytes = query.allocatedBytes();
        this.pageHits = query.pageHits();
        this.pageFaults = query.pageFaults();
    }

    private static String formatTime( final long startTime )
    {
        return OffsetDateTime
                .ofInstant( Instant.ofEpochMilli( startTime ), UTC_ZONE_ID)
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
