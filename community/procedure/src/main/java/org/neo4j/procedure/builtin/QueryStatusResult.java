/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.procedure.builtin;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.core.TransactionalEntityFactory;
import org.neo4j.kernel.impl.util.BaseToObjectValueWriter;
import org.neo4j.memory.OptionalMemoryTracker;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.virtual.MapValue;

import static java.util.Collections.singletonList;

public class QueryStatusResult
{
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
    /** @since Neo4j 3.5 */
    public final String connectionId;
    /** @since Neo4j 4.0 */
    public final String database;

    QueryStatusResult( ExecutingQuery query, TransactionalEntityFactory manager, ZoneId zoneId, String database ) throws InvalidArgumentsException
    {
        this( query.snapshot(), manager, zoneId, database );
    }

    private QueryStatusResult( QuerySnapshot query, TransactionalEntityFactory manager, ZoneId zoneId, String database ) throws InvalidArgumentsException
    {
        this.queryId = new QueryId( query.internalQueryId() ).toString();
        this.username = query.username();
        this.query = query.obfuscatedQueryText().orElse( null );
        this.database = database;
        this.parameters = asRawMap( query.obfuscatedQueryParameters().orElse( MapValue.EMPTY ), new ParameterWriter( manager ) );
        this.startTime = ProceduresTimeFormatHelper.formatTime( query.startTimestampMillis(), zoneId );
        this.elapsedTimeMillis = asMillis( query.elapsedTimeMicros() );
        ClientConnectionInfo clientConnection = query.clientConnection();
        this.protocol = clientConnection.protocol();
        this.clientAddress = clientConnection.clientAddress();
        this.requestUri = clientConnection.requestURI();
        this.metaData = query.transactionAnnotationData();
        this.cpuTimeMillis = asMillis( query.cpuTimeMicros() );
        this.status = query.status();
        this.resourceInformation = query.resourceInformation();
        this.activeLockCount = query.activeLockCount();
        this.waitTimeMillis = asMillis( query.waitTimeMicros() );
        this.idleTimeMillis = asMillis( query.idleTimeMicros() );
        this.planner = query.planner();
        this.runtime = query.runtime();
        this.indexes = query.indexes();
        long bytes = query.allocatedBytes();
        this.allocatedBytes = bytes == OptionalMemoryTracker.ALLOCATIONS_NOT_TRACKED ? null : bytes;
        this.pageHits = query.pageHits();
        this.pageFaults = query.pageFaults();
        this.connectionId = clientConnection.connectionId();
    }

    private Long asMillis( Long micros )
    {
        return micros == null ? null : TimeUnit.MICROSECONDS.toMillis( micros );
    }

    private static Map<String,Object> asRawMap( MapValue mapValue, ParameterWriter writer )
    {
        HashMap<String,Object> map = new HashMap<>();
        mapValue.foreach( ( s, value ) ->
        {
            value.writeTo( writer );
            map.put( s, writer.value() );
        } );
        return map;
    }

    private static class ParameterWriter extends BaseToObjectValueWriter<RuntimeException>
    {
        private final TransactionalEntityFactory entityFactory;

        private ParameterWriter( TransactionalEntityFactory entityFactory )
        {
            this.entityFactory = entityFactory;
        }

        @Override
        protected Node newNodeEntityById( long id )
        {
            return entityFactory.newNodeEntity( id );
        }

        @Override
        protected Relationship newRelationshipEntityById( long id )
        {
            return entityFactory.newRelationshipEntity( id );
        }

        @Override
        protected Point newPoint( CoordinateReferenceSystem crs, double[] coordinate )
        {
            return new Point()
            {
                @Override
                public String getGeometryType()
                {
                    return "Point";
                }

                @Override
                public List<Coordinate> getCoordinates()
                {
                    return singletonList( new Coordinate( coordinate ) );
                }

                @Override
                public CRS getCRS()
                {
                    return crs;
                }
            };
        }
    }
}
