package org.neo4j.kernel.enterprise.builtinprocs;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Map;

import org.neo4j.kernel.api.ExecutingQuery;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.impl.query.QuerySource;

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
    public final String startTime;
    public final String elapsedTime;
    public final String connectionDetails;
    public final long cpuTime;
    public final Map<String,Object> status;
    public final long waitTime;
    public final Map<String,Object> metaData;

    QueryStatusResult( ExecutingQuery q ) throws InvalidArgumentsException
    {
        this(
                ofInternalId( q.internalQueryId() ),
                q.username(),
                q.queryText(),
                q.queryParameters(),
                q.startTime(),
                q.elapsedTime(),
                q.querySource(),
                q.metaData(),
                q.cpuTime(),
                q.status(),
                q.waitTime() );
    }

    private QueryStatusResult(
            QueryId queryId,
            String username,
            String query,
            Map<String,Object> parameters,
            long startTime,
            long elapsedTime,
            QuerySource querySource,
            Map<String,Object> txMetaData,
            long cpuTime,
            Map<String,Object> status,
            long waitTime
    ) {
        this.queryId = queryId.toString();
        this.username = username;
        this.query = query;
        this.parameters = parameters;
        this.startTime = formatTime( startTime );
        this.elapsedTime = formatInterval( elapsedTime );
        this.connectionDetails = querySource.toString();
        this.metaData = txMetaData;
        this.cpuTime = cpuTime;
        this.status = status;
        this.waitTime = waitTime;
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
