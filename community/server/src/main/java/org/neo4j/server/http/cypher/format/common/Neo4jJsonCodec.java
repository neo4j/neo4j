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
package org.neo4j.server.http.cypher.format.common;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.lang.reflect.Array;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAmount;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.spatial.CRS;
import org.neo4j.graphdb.spatial.Coordinate;
import org.neo4j.graphdb.spatial.Geometry;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.server.http.cypher.TransactionHandle;
import org.neo4j.server.http.cypher.TransactionStateChecker;
import org.neo4j.server.http.cypher.TransitionalTxManagementKernelTransaction;

import static com.fasterxml.jackson.databind.SerializationFeature.FLUSH_AFTER_WRITE_VALUE;
import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.collection.MapUtil.genericMap;

public class Neo4jJsonCodec extends ObjectMapper
{
    private enum Neo4jJsonMetaType
    {
        NODE( "node" ),
        RELATIONSHIP( "relationship" ),
        DATE_TIME( "datetime" ),
        TIME( "time" ),
        LOCAL_DATE_TIME( "localdatetime" ),
        DATE( "date" ),
        LOCAL_TIME( "localtime" ),
        DURATION( "duration" ),
        POINT( "point" );

        private final String code;

        Neo4jJsonMetaType( final String code )
        {
            this.code = code;
        }

        String code()
        {
            return this.code;
        }
    }

    private final TransactionHandle transactionHandle;

    public Neo4jJsonCodec( TransactionHandle transactionHandle )
    {
        this.transactionHandle = transactionHandle;
        getSerializationConfig().without( FLUSH_AFTER_WRITE_VALUE );
    }

    public Neo4jJsonCodec()
    {
        this( null );
    }

    @Override
    public void writeValue( JsonGenerator out, Object value ) throws IOException
    {
        if ( value instanceof Entity )
        {
            var context = transactionHandle.getContext();
            TransactionStateChecker txStateChecker = TransactionStateChecker.create( context );
            writeEntity( out, (Entity) value, txStateChecker, context );
        }
        else if ( value instanceof Path )
        {
            var context = transactionHandle.getContext();
            TransactionStateChecker txStateChecker = TransactionStateChecker.create( context );
            writePath( out, ((Path) value).iterator(), txStateChecker, context );
        }
        else if ( value instanceof Iterable )
        {
            writeIterator( out, ((Iterable) value).iterator() );
        }
        else if ( value instanceof byte[] )
        {
            writeByteArray( out, (byte[]) value );
        }
        else if ( value instanceof Map )
        {
            writeMap(out, (Map) value );
        }
        else if ( value instanceof Geometry )
        {
            Geometry geom = (Geometry) value;
            Object coordinates = (geom instanceof Point) ? ((Point) geom).getCoordinate() : geom.getCoordinates();
            writeMap( out, genericMap( new LinkedHashMap<>(), "type", geom.getGeometryType(), "coordinates", coordinates, "crs", geom.getCRS() ) );
        }
        else if ( value instanceof Coordinate )
        {
            Coordinate coordinate = (Coordinate) value;
            writeIterator( out, coordinate.getCoordinate().iterator() );
        }
        else if ( value instanceof CRS )
        {
            CRS crs = (CRS) value;
            writeMap( out, genericMap( new LinkedHashMap<>(), "srid", crs.getCode(), "name", crs.getType(), "type", "link", "properties",
                    genericMap( new LinkedHashMap<>(), "href", crs.getHref() + "ogcwkt/", "type", "ogcwkt" ) ) );
        }
        else if ( value instanceof Temporal || value instanceof TemporalAmount )
        {
            super.writeValue( out, value.toString() );
        }
        else if ( value != null && value.getClass().isArray() && supportedArrayType( value.getClass().getComponentType() ) )
        {
            writeReflectiveArray( out, value );
        }
        else
        {
            super.writeValue( out, value );
        }
    }

    private boolean supportedArrayType( Class<?> valueClass )
    {
        return Geometry.class.isAssignableFrom( valueClass ) || CRS.class.isAssignableFrom( valueClass ) ||
               Temporal.class.isAssignableFrom( valueClass ) || TemporalAmount.class.isAssignableFrom( valueClass );
    }

    private void writeReflectiveArray( JsonGenerator out, Object array ) throws IOException
    {
        out.writeStartArray();
        try
        {
            int length = Array.getLength( array );
            for ( int i = 0; i < length; i++ )
            {
                writeValue( out, Array.get( array, i )  );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writeMap( JsonGenerator out, Map value ) throws IOException
    {
        out.writeStartObject();
        try
        {
            Set<Map.Entry> set = value.entrySet();
            for ( Map.Entry e : set )
            {
                Object key = e.getKey();
                out.writeFieldName( key == null ? "null" : key.toString() );
                writeValue( out, e.getValue() );
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeIterator( JsonGenerator out, Iterator value ) throws IOException
    {
        out.writeStartArray();
        try
        {
            while ( value.hasNext() )
            {
                writeValue( out, value.next() );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writePath( JsonGenerator out, Iterator<Entity> value, TransactionStateChecker txStateChecker,
            TransitionalTxManagementKernelTransaction context ) throws IOException
    {
        out.writeStartArray();
        try
        {
            while ( value.hasNext() )
            {
                writeEntity( out, value.next(), txStateChecker, context );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writeEntity( JsonGenerator out, Entity value, TransactionStateChecker txStateChecker, TransitionalTxManagementKernelTransaction context )
            throws IOException
    {
        var transaction = context.getInternalTransaction();
        if ( value instanceof Node )
        {
            var nodeDeletedInCurrentTx = txStateChecker.isNodeDeletedInCurrentTx( value.getId() );
            var entity = !nodeDeletedInCurrentTx ? transaction.getNodeById( value.getId() ) : value;
            writeNodeOrRelationship( out, entity, nodeDeletedInCurrentTx );
        }
        else if ( value instanceof Relationship )
        {
            var relationshipDeletedInCurrentTx = txStateChecker.isRelationshipDeletedInCurrentTx( value.getId() );
            var entity = !relationshipDeletedInCurrentTx ? transaction.getRelationshipById( value.getId() ) : value;
            writeNodeOrRelationship( out, entity, relationshipDeletedInCurrentTx );
        }
        else
        {
            throw new IllegalArgumentException( "Expected a Node or Relationship, but got a " + value );
        }
    }

    private void writeNodeOrRelationship( JsonGenerator out, Entity entity, boolean isDeleted )
            throws IOException
    {
        out.writeStartObject();
        try
        {
            if ( !isDeleted )
            {
                for ( Map.Entry<String,Object> property : entity.getAllProperties().entrySet() )
                {
                    out.writeObjectField( property.getKey(), property.getValue() );
                }
            }
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeByteArray( JsonGenerator out, byte[] bytes ) throws IOException
    {
        out.writeStartArray();
        try
        {
            for ( byte b : bytes )
            {
                out.writeNumber( b );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    public void writeMeta( JsonGenerator out, Object value ) throws IOException
    {
        if ( value instanceof Node )
        {
            Node node = (Node) value;
            TransactionStateChecker stateChecker = TransactionStateChecker.create( transactionHandle.getContext() );
            writeNodeOrRelationshipMeta( out, node.getId(), Neo4jJsonMetaType.NODE, stateChecker.isNodeDeletedInCurrentTx( node.getId() ) );
        }
        else if ( value instanceof Relationship )
        {
            Relationship relationship = (Relationship) value;
            TransactionStateChecker transactionStateChecker = TransactionStateChecker.create( transactionHandle.getContext() );
            writeNodeOrRelationshipMeta( out, relationship.getId(), Neo4jJsonMetaType.RELATIONSHIP,
                    transactionStateChecker.isRelationshipDeletedInCurrentTx( relationship.getId() ) );
        }
        else if ( value instanceof Path )
        {
            writeMetaPath( out, (Path) value );
        }
        else if ( value instanceof Iterable )
        {
            for ( Object v : (Iterable) value )
            {
                writeMeta( out, v );
            }
        }
        else if ( value instanceof Map )
        {
            Map map = (Map) value;
            for ( var mapValue : map.values() )
            {
                writeMeta( out, mapValue );
            }
        }
        else if ( value instanceof Geometry )
        {
            writeObjectMeta( out, parseGeometryType( (Geometry) value ) );
        }
        else if ( value instanceof Temporal )
        {
            writeObjectMeta( out, parseTemporalType( (Temporal) value ) );
        }
        else if ( value instanceof TemporalAmount )
        {
            writeObjectMeta( out, Neo4jJsonMetaType.DURATION );
        }
        else
        {
            out.writeNull();
        }
    }

    private Neo4jJsonMetaType parseGeometryType( Geometry value )
    {
        Neo4jJsonMetaType type = null;
        if ( value instanceof Point )
        {
            type = Neo4jJsonMetaType.POINT;
        }
        if ( type == null )
        {
            throw new IllegalArgumentException(
                    String.format( "Unsupported Geometry type: type=%s, value=%s", value.getClass().getSimpleName(), value ) );
        }
        return type;
    }

    private Neo4jJsonMetaType parseTemporalType( Temporal value )
    {
        Neo4jJsonMetaType type = null;
        if ( value instanceof ZonedDateTime )
        {
            type = Neo4jJsonMetaType.DATE_TIME;
        }
        else if ( value instanceof LocalDate )
        {
            type = Neo4jJsonMetaType.DATE;
        }
        else if ( value instanceof OffsetTime )
        {
            type = Neo4jJsonMetaType.TIME;
        }
        else if ( value instanceof LocalDateTime )
        {
            type = Neo4jJsonMetaType.LOCAL_DATE_TIME;
        }
        else if ( value instanceof LocalTime )
        {
            type = Neo4jJsonMetaType.LOCAL_TIME;
        }
        if ( type == null )
        {
            throw new IllegalArgumentException(
                    String.format( "Unsupported Temporal type: type=%s, value=%s", value.getClass().getSimpleName(), value ) );
        }
        return type;
    }

    private void writeMetaPath( JsonGenerator out, Path value ) throws IOException
    {
        out.writeStartArray();
        try
        {
            for ( Entity element : value )
            {
                writeMeta( out, element );
            }
        }
        finally
        {
            out.writeEndArray();
        }
    }

    private void writeObjectMeta( JsonGenerator out, Neo4jJsonMetaType type )
            throws IOException
    {
        requireNonNull( type, "The meta type cannot be null for known types." );
        out.writeStartObject();
        try
        {
            out.writeStringField( "type", type.code() );
        }
        finally
        {
            out.writeEndObject();
        }
    }

    private void writeNodeOrRelationshipMeta( JsonGenerator out, long id, Neo4jJsonMetaType type, boolean isDeleted )
            throws IOException
    {
        requireNonNull( type, "The meta type could not be null for node or relationship." );
        out.writeStartObject();
        try
        {
            out.writeNumberField( "id", id );
            out.writeStringField( "type", type.code() );
            out.writeBooleanField( "deleted", isDeleted );
        }
        finally
        {
            out.writeEndObject();
        }
    }
}
