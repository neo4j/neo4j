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
package org.neo4j.cypher.operations;

import org.neo4j.cypher.internal.v3_5.util.CypherTypeException;
import org.neo4j.cypher.internal.v3_5.util.InvalidArgumentException;
import org.neo4j.cypher.internal.v3_5.util.ParameterWrongTypeException;

import java.math.BigDecimal;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.utils.InvalidValuesArgumentException;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.Double.parseDouble;
import static java.lang.Long.parseLong;
import static java.lang.String.format;
import static org.neo4j.values.storable.Values.EMPTY_STRING;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_LIST;

/**
 * This class contains static helper methods for the set of Cypher functions
 */
@SuppressWarnings( "unused" )
public final class CypherFunctions
{
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf( Long.MAX_VALUE );
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf( Long.MIN_VALUE );
    private static String[] POINT_KEYS = new String[]{"crs", "x", "y", "z", "longitude", "latitude", "height", "srid"};

    private CypherFunctions()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static DoubleValue sin( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.sin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "sin()" );
        }
    }

    public static DoubleValue asin( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.asin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "asin()" );
        }
    }

    public static DoubleValue haversin( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( (1.0 - Math.cos( ((NumberValue) in).doubleValue() )) / 2 );
        }
        else
        {
            throw needsNumbers( "haversin()" );
        }
    }

    public static DoubleValue cos( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.cos( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "cos()" );
        }
    }

    public static DoubleValue cot( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( 1.0 / Math.tan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "cot()" );
        }
    }

    public static DoubleValue acos( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.acos( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "acos()" );
        }
    }

    public static DoubleValue tan( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.tan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "tan()" );
        }
    }

    public static DoubleValue atan( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.atan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "atan()" );
        }
    }

    public static DoubleValue atan2( AnyValue y, AnyValue x )
    {
        if ( y instanceof NumberValue && x instanceof NumberValue )
        {
            return doubleValue( Math.atan2( ((NumberValue) y).doubleValue(), ((NumberValue) x).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "atan2()" );
        }
    }

    public static DoubleValue ceil( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.ceil( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "ceil()" );
        }
    }

    public static DoubleValue floor( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.floor( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "floor()" );
        }
    }

    public static DoubleValue round( AnyValue in )
    {
       if ( in instanceof NumberValue )
        {
            return doubleValue( Math.round( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "round()" );
        }
    }

    public static NumberValue abs( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            if ( in instanceof IntegralValue )
            {
                return longValue( Math.abs( ((NumberValue) in).longValue() ) );
            }
            else
            {
                return doubleValue( Math.abs( ((NumberValue) in).doubleValue() ) );
            }
        }
        else
        {
            throw needsNumbers( "abs()" );
        }
    }

    public static DoubleValue toDegrees( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.toDegrees( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "toDegrees()" );
        }
    }

    public static DoubleValue exp( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.exp( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "exp()" );
        }
    }

    public static DoubleValue log( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.log( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "log()" );
        }
    }

    public static DoubleValue log10( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.log10( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "log10()" );
        }
    }

    public static DoubleValue toRadians( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.toRadians( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "toRadians()" );
        }
    }

    public static ListValue range( AnyValue startValue, AnyValue endValue )
    {
        return VirtualValues.range( asLong( startValue ), asLong( endValue ), 1L );
    }

    public static ListValue range( AnyValue startValue, AnyValue endValue, AnyValue stepValue )
    {
        long step = asLong( stepValue );
        if ( step == 0L )
        {
            throw new InvalidArgumentException( "step argument to range() cannot be zero", null );
        }

        return VirtualValues.range( asLong( startValue ), asLong( endValue ), step );
    }

    public static LongValue signum( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return longValue( (long) Math.signum( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "signum()" );
        }
    }

    public static DoubleValue sqrt( AnyValue in )
    {
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.sqrt( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "sqrt()" );
        }
    }

    public static DoubleValue rand()
    {
        return doubleValue( ThreadLocalRandom.current().nextDouble() );
    }

    // TODO: Support better calculations, like https://en.wikipedia.org/wiki/Vincenty%27s_formulae
    // TODO: Support more coordinate systems
    public static Value distance( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof PointValue && rhs instanceof PointValue )
        {
            return calculateDistance( (PointValue) lhs, (PointValue) rhs );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static NodeValue startNode( AnyValue anyValue, DbAccess access )
    {
        if ( anyValue instanceof RelationshipValue )
        {
            return access.relationshipGetStartNode( (RelationshipValue) anyValue );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a RelationshipValue", anyValue), null );
        }
    }

    public static NodeValue endNode( AnyValue anyValue, DbAccess access )
    {
        if ( anyValue instanceof RelationshipValue )
        {
            return access.relationshipGetEndNode( (RelationshipValue) anyValue );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a RelationshipValue", anyValue), null );
        }
    }

    public static BooleanValue propertyExists( String key, AnyValue container, DbAccess dbAccess )
    {
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeHasProperty( ((VirtualNodeValue) container).id(), dbAccess.propertyKey( key ) ) ? TRUE : FALSE;
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipHasProperty( ((VirtualRelationshipValue) container).id(),
                    dbAccess.propertyKey( key ) ) ? TRUE : FALSE;
        }
        else if ( container instanceof MapValue )
        {
            return ((MapValue) container).get( key ) != NO_VALUE ? TRUE : FALSE;
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a property container", container), null );
        }
    }

    public static AnyValue propertyGet( String key, AnyValue container, DbAccess dbAccess )
    {
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeProperty( ((VirtualNodeValue) container).id(), dbAccess.propertyKey( key ) );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess
                    .relationshipProperty( ((VirtualRelationshipValue) container).id(), dbAccess.propertyKey( key ) );
        }
        else if ( container instanceof MapValue )
        {
            return ((MapValue) container).get( key );
        }
        else if ( container instanceof TemporalValue<?,?> )
        {
            return ((TemporalValue) container).get( key );
        }
        else if ( container instanceof DurationValue )
        {
            return ((DurationValue) container).get( key );
        }
        else if ( container instanceof PointValue )
        {
            try
            {
                return ((PointValue) container).get( key );
            }
            catch ( InvalidValuesArgumentException e )
            {
                throw new InvalidArgumentException( e.getMessage(), e );
            }
        }
        else
        {
            throw new CypherTypeException( format( "Type mismatch: expected a map but was %s", container.toString() ), null );
        }
    }

    public static AnyValue containerIndex( AnyValue container, AnyValue index, DbAccess dbAccess )
    {
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeProperty( ((VirtualNodeValue) container).id(), dbAccess.propertyKey( asString( index ) ) );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipProperty( ((VirtualRelationshipValue) container).id(),
                    dbAccess.propertyKey( asString( index ) ) );
        }
        if ( container instanceof MapValue )
        {
            return mapAccess( (MapValue) container, index );
        }
        else if ( container instanceof SequenceValue )
        {
            return listAccess( (SequenceValue) container, index );
        }
        else
        {
            throw new CypherTypeException( format(
                    "`%s` is not a collection or a map. Element access is only possible by performing a collection " +
                    "lookup using an integer index, or by performing a map lookup using a string key (found: %s[%s])",
                    container, container, index ), null );
        }
    }

    public static AnyValue head( AnyValue container )
    {
        if ( container instanceof SequenceValue )
        {
            SequenceValue sequence = (SequenceValue) container;
            if ( sequence.length() == 0 )
            {
                return NO_VALUE;
            }

            return sequence.value( 0 );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a list", container ), null );
        }
    }

    public static ListValue tail( AnyValue container )
    {
       if ( container instanceof ListValue )
       {
           return ((ListValue) container).tail();
       }
       else if ( container instanceof ArrayValue )
       {
           return VirtualValues.fromArray( (ArrayValue) container ).tail();
       }
       else
       {
           return EMPTY_LIST;
       }
    }

    public static AnyValue last( AnyValue container )
    {
        if ( container instanceof SequenceValue )
        {
            SequenceValue sequence = (SequenceValue) container;
            int length = sequence.length();
            if ( length == 0 )
            {
                return NO_VALUE;
            }

            return sequence.value( length - 1 );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a list", container ), null );
        }
    }

    public static TextValue left( AnyValue in, AnyValue endPos )
    {
        if ( in instanceof TextValue )
        {
            int len = asInt( endPos );
            return ((TextValue) in).substring( 0, len );
        }
        else
        {
            throw notAString( "left", in );
        }
    }

    public static TextValue ltrim( AnyValue in )
    {
        if ( in instanceof TextValue )
        {
            return ((TextValue) in).ltrim();
        }
        else
        {
            throw notAString( "ltrim", in );
        }
    }

    public static TextValue rtrim( AnyValue in )
    {
        if ( in instanceof TextValue )
        {
            return ((TextValue) in).rtrim();
        }
        else
        {
            throw notAString( "rtrim", in );
        }
    }

    public static TextValue trim( AnyValue in )
    {
        if ( in instanceof TextValue )
        {
            return ((TextValue) in).trim();
        }
        else
        {
            throw notAString( "trim", in );
        }
    }

    public static TextValue replace( AnyValue original, AnyValue search, AnyValue replaceWith )
    {
        if ( original instanceof TextValue )
        {
            return ((TextValue) original).replace( asString( search ), asString( replaceWith ) );
        }
        else
        {
            throw notAString( "replace", original );
        }
    }

    public static AnyValue reverse( AnyValue original )
    {
        if ( original instanceof TextValue )
        {
            return ((TextValue) original).reverse();
        }
        else if ( original instanceof ListValue )
        {
            return ((ListValue) original).reverse();
        }
        else
        {
            throw new CypherTypeException(
                    "Expected a string or a list; consider converting it to a string with toString() or creating a list.",
                    null );
        }
    }

    public static TextValue right( AnyValue original, AnyValue length )
    {
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;
            int len = asInt( length );
            if ( len < 0 )
            {
                throw new IndexOutOfBoundsException( "negative length" );
            }
            int startVal = asText.length() - len;
            return asText.substring( Math.max( 0, startVal ) );
        }
        else
        {
            throw notAString( "right", original );
        }
    }

    public static ListValue split( AnyValue original, AnyValue separator )
    {
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;
            if ( asText.length() == 0 )
            {
                return VirtualValues.list( EMPTY_STRING );
            }
            return asText.split( asString( separator ) );
        }
        else
        {
            throw notAString( "split", original );
        }
    }

    public static TextValue substring( AnyValue original, AnyValue start )
    {
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;

            return asText.substring( asInt( start ));
        }
        else
        {
            throw notAString( "substring", original );
        }
    }

    public static TextValue substring( AnyValue original, AnyValue start, AnyValue length )
    {
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;

            return asText.substring( asInt( start ), asInt( length ));
        }
        else
        {
            throw notAString( "substring", original );
        }
    }

    public static TextValue toLower( AnyValue in )
    {
        if ( in instanceof TextValue )
        {
            return ((TextValue) in).toLower();
        }
        else
        {
            throw notAString( "toLower", in );
        }
    }

    public static TextValue toUpper( AnyValue in )
    {
        if ( in instanceof TextValue )
        {
            return ((TextValue) in).toUpper();
        }
        else
        {
            throw notAString( "toUpper", in );
        }
    }

    public static LongValue id( AnyValue item )
    {
        if ( item instanceof VirtualNodeValue )
        {
            return longValue( ((VirtualNodeValue) item).id() );
        }
        else if ( item instanceof VirtualRelationshipValue )
        {
            return longValue( ((VirtualRelationshipValue) item).id() );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a node or relationship, but it was `%s`",
                    item, item.getClass().getSimpleName() ), null );

        }
    }

    public static ListValue labels( AnyValue item, DbAccess access )
    {
        if ( item instanceof NodeValue )
        {
            return access.getLabelsForNode( ((NodeValue) item).id() );
        }
        else
        {
            throw new ParameterWrongTypeException( "Expected a Node, got: " + item, null );
        }
    }

    public static boolean hasLabel( AnyValue entity, int labelToken, DbAccess access )
    {
        if ( entity instanceof NodeValue )
        {
            return access.isLabelSetOnNode( labelToken, ((NodeValue) entity).id() );
        }
        else
        {
            throw new ParameterWrongTypeException( "Expected a Node, got: " + entity, null );
        }
    }

    public static TextValue type( AnyValue item )
    {
        if ( item instanceof RelationshipValue )
        {
            return ((RelationshipValue) item).type();
        }
        else
        {
            throw new ParameterWrongTypeException( "Expected a Relationship, got: " + item, null );
        }
    }

    public static ListValue nodes( AnyValue in )
    {
        if ( in instanceof PathValue )
        {
            return VirtualValues.list( ((PathValue) in).nodes() );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a path.", in ), null );
        }
    }

    public static ListValue relationships( AnyValue in )
    {
        if ( in instanceof PathValue )
        {
            return VirtualValues.list( ((PathValue) in).relationships() );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a path.", in ), null );
        }
    }

    public static Value point( AnyValue in, DbAccess access )
    {
        if ( in instanceof VirtualNodeValue )
        {
            return asPoint( access, (VirtualNodeValue) in );
        }
        else if ( in instanceof VirtualRelationshipValue )
        {
            return asPoint( access, (VirtualRelationshipValue) in );
        }
        else if ( in instanceof MapValue )
        {
            MapValue map = (MapValue) in;
            if ( containsNull( map ) )
            {
                return NO_VALUE;
            }
            return PointValue.fromMap( map );
        }
        else
        {
            throw new CypherTypeException( format( "Expected a map but got %s", in ), null );
        }
    }

    public static ListValue keys( AnyValue in, DbAccess access )
    {
        if ( in instanceof VirtualNodeValue )
        {
            return extractKeys( access, access.nodePropertyIds( ((VirtualNodeValue) in).id() ) );
        }
        else if ( in instanceof VirtualRelationshipValue )
        {
            return extractKeys( access, access.relationshipPropertyIds( ((VirtualRelationshipValue) in).id() ) );
        }
        else if ( in instanceof MapValue )
        {
            return ((MapValue) in).keys();
        }
        else
        {
            throw new CypherTypeException( format( "Expected a node, a relationship or a literal map but got %s", in ), null );
        }
    }

    public static MapValue properties( AnyValue in, DbAccess access )
    {
        if ( in instanceof VirtualNodeValue )
        {
            return access.nodeAsMap( ((VirtualNodeValue) in).id() );
        }
        else if ( in instanceof VirtualRelationshipValue )
        {
           return access.relationshipAsMap( ((VirtualRelationshipValue) in).id() );
        }
        else if ( in instanceof MapValue )
        {
            return (MapValue) in;
        }
        else
        {
            throw new CypherTypeException( format( "Expected a node, a relationship or a literal map but got %s", in ), null );
        }
    }

    public static IntegralValue size( AnyValue item )
    {
        if ( item instanceof PathValue )
        {
            throw new CypherTypeException( "SIZE cannot be used on paths", null );
        }
        else if ( item instanceof TextValue )
        {
            return longValue( ((TextValue) item).length() );
        }
        else if ( item instanceof SequenceValue )
        {
            return longValue( ((SequenceValue) item).length() );
        }
        else
        {
            return longValue( 1 );
        }
    }

    //NOTE all usage except for paths is deprecated
    public static IntegralValue length( AnyValue item )
    {
        if ( item instanceof PathValue )
        {
            return longValue( ((PathValue) item).size() );
        }
        else if ( item instanceof TextValue )
        {
            return longValue( ((TextValue) item).length() );
        }
        else if ( item instanceof SequenceValue )
        {
            return longValue( ((SequenceValue) item).length() );
        }
        else
        {
            return longValue( 1 );
        }
    }

    public static Value toBoolean( AnyValue in )
    {
        if ( in instanceof BooleanValue )
        {
            return (BooleanValue) in;
        }
        else if ( in instanceof TextValue )
        {
            switch ( ((TextValue) in).trim().stringValue().toLowerCase() )
            {
            case "true":
                return TRUE;
            case "false":
                return FALSE;
            default:
                return NO_VALUE;
            }
        }
        else
        {
            throw new ParameterWrongTypeException( "Expected a Boolean or String, got: " + in.toString(), null );
        }
    }

    public static Value toFloat( AnyValue in )
    {
        if ( in instanceof DoubleValue )
        {
            return (DoubleValue) in;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( ((NumberValue) in).doubleValue() );
        }
        else if ( in instanceof TextValue )
        {
            try
            {
                return doubleValue( parseDouble( ((TextValue) in).stringValue() ) );
            }
            catch ( NumberFormatException ignore )
            {
                return NO_VALUE;
            }
        }
        else
        {
            throw new ParameterWrongTypeException( "Expected a String or Number, got: " + in.toString(), null );
        }
    }

    public static Value toInteger( AnyValue in )
    {
        if ( in instanceof IntegralValue )
        {
            return (IntegralValue) in;
        }
        else if ( in instanceof NumberValue )
        {
            return longValue( ((NumberValue) in).longValue() );
        }
        else if ( in instanceof TextValue )
        {
            return stringToLongValue( (TextValue) in );
        }
        else
        {
            throw new ParameterWrongTypeException( "Expected a String or Number, got: " + in.toString(), null );
        }
    }

    public static TextValue toString( AnyValue in )
    {
        if ( in instanceof TextValue )
        {
            return (TextValue) in;
        }
        else if ( in instanceof NumberValue )
        {
            return stringValue( ((NumberValue) in).prettyPrint() );
        }
        else if ( in instanceof BooleanValue )
        {
            return stringValue( ((BooleanValue) in).prettyPrint() );
        }
        else if ( in instanceof TemporalValue || in instanceof DurationValue || in instanceof PointValue )
        {
            return stringValue( in.toString() );
        }
        else
        {
            throw new ParameterWrongTypeException(
                    "Expected a String, Number, Boolean, Temporal or Duration, got: " + in.toString(), null );
        }
    }

    public static ListValue fromSlice( AnyValue collection, AnyValue fromValue )
    {
       int from = asInt( fromValue );
       ListValue list = makeTraversable( collection );
       if ( from >= 0 )
       {
           return list.drop( from );
       }
       else
       {
           return list.drop( list.size() + from );
       }
    }

    public static ListValue toSlice( AnyValue collection, AnyValue fromValue )
    {
        int from = asInt( fromValue );
        ListValue list = makeTraversable( collection );
        if ( from >= 0 )
        {
            return list.take( from );
        }
        else
        {
            return list.take( list.size() + from );
        }
    }

    public static ListValue fullSlice( AnyValue collection, AnyValue fromValue, AnyValue toValue )
    {
        int from = asInt( fromValue );
        int to = asInt( toValue );
        ListValue list = makeTraversable( collection );
        int size = list.size();
        if ( from >= 0 && to >= 0 )
        {
            return list.slice( from, to );
        }
        else if ( from >= 0 )
        {
            return list.slice( from, size + to );
        }
        else if ( to >= 0 )
        {
            return list.slice( size + from, to );
        }
        else
        {
            return list.slice( size + from, size + to );
        }
    }

    public static ListValue makeTraversable( AnyValue collection )
    {
        ListValue list;
        if ( collection == NO_VALUE )
        {
            return VirtualValues.EMPTY_LIST;
        }
        else if ( collection instanceof ListValue )
        {
            return  (ListValue) collection;
        }
        else if ( collection instanceof ArrayValue )
        {
            return VirtualValues.fromArray( (ArrayValue) collection );
        }
        else
        {
            return VirtualValues.list( collection );
        }
    }

    private static Value stringToLongValue( TextValue in )
    {
        try
        {
            return longValue( parseLong( in.stringValue() ) );
        }

        catch ( Exception e )
        {
            try
            {
                BigDecimal bigDecimal = new BigDecimal( in.stringValue() );
                if ( bigDecimal.compareTo( MAX_LONG ) <= 0 && bigDecimal.compareTo( MIN_LONG ) >= 0 )
                {
                    return longValue( bigDecimal.longValue() );
                }
                else
                {
                    throw new CypherTypeException( format( "integer, %s, is too large", in.stringValue() ), null );
                }
            }
            catch ( NumberFormatException ignore )
            {
                return NO_VALUE;
            }
        }
    }

    private static ListValue extractKeys( DbAccess access, int[] keyIds )
    {
        String[] keysNames = new String[keyIds.length];
        for ( int i = 0; i < keyIds.length; i++ )
        {
            keysNames[i] = access.getPropertyKeyName( keyIds[i] );
        }
        return VirtualValues.fromArray( Values.stringArray( keysNames ) );
    }

    private static Value asPoint( DbAccess access, VirtualNodeValue nodeValue )
    {
        MapValueBuilder builder = new MapValueBuilder();
        for ( String key : POINT_KEYS )
        {
            Value value = access.nodeProperty( nodeValue.id(), access.propertyKey( key ) );
            if ( value == NO_VALUE )
            {
                continue;
            }
            builder.add( key, value );
        }

        return PointValue.fromMap( builder.build() );
    }

    private static Value asPoint( DbAccess access, VirtualRelationshipValue relationshipValue )
    {
        MapValueBuilder builder = new MapValueBuilder();
        for ( String key : POINT_KEYS )
        {
            Value value = access.relationshipProperty( relationshipValue.id(), access.propertyKey( key ) );
            if ( value == NO_VALUE )
            {
                continue;
            }
            builder.add( key, value );
        }

        return PointValue.fromMap( builder.build() );
    }

    private static boolean containsNull( MapValue map )
    {
        boolean[] hasNull = {false};
        map.foreach( ( s, value ) -> {
            if ( value == NO_VALUE )
            {
                hasNull[0] = true;
            }
        } );
        return hasNull[0];
    }

    private static AnyValue listAccess( SequenceValue container, AnyValue index )
    {
        NumberValue number = asNumberValue( index );
        if ( !(number instanceof IntegralValue) )
        {
            throw new CypherTypeException( format( "Cannot index a list using an non-integer number, got %s", number ),
                    null );
        }
        long idx = number.longValue();
        if ( idx > Integer.MAX_VALUE || idx < Integer.MIN_VALUE )
        {
            throw new InvalidArgumentException(
                    format( "Cannot index a list using a value greater than %d or lesser than %d, got %d",
                            Integer.MAX_VALUE, Integer.MIN_VALUE, idx ), null );
        }

        if ( idx < 0 )
        {
            idx = container.length() + idx;
        }
        if ( idx >= container.length() || idx < 0 )
        {
            return NO_VALUE;
        }
        return container.value( (int) idx );
    }

    private static AnyValue mapAccess( MapValue container, AnyValue index )
    {

        return container.get( asString( index ) );
    }

    public static TextValue asTextValue( AnyValue value )
    {
        if ( !(value instanceof TextValue) )
        {
            throw new CypherTypeException( format( "Expected %s to be a %s, but it was a %s", value,
                    TextValue.class.getName(), value.getClass().getName() ), null );
        }
        return (TextValue) value;
    }

    static String asString( AnyValue value )
    {
       return asTextValue( value ).stringValue();
    }

    private static NumberValue asNumberValue( AnyValue value )
    {
        if ( !(value instanceof NumberValue) )
        {
            throw new CypherTypeException( format( "Expected %s to be a %s, but it was a %s", value,
                    NumberValue.class.getName(), value.getClass().getName() ), null );
        }
        return (NumberValue) value;
    }

    private static Value calculateDistance( PointValue p1, PointValue p2 )
    {
        if ( p1.getCoordinateReferenceSystem().equals( p2.getCoordinateReferenceSystem() ) )
        {
            return doubleValue( p1.getCoordinateReferenceSystem().getCalculator().distance( p1, p2 ) );
        }
        else
        {
            return NO_VALUE;
        }
    }

    private static long asLong( AnyValue value )
    {
        if ( value instanceof NumberValue )
        {
            return ((NumberValue) value).longValue();
        }
        else
        {
            throw new CypherTypeException( "Expected a numeric value but got: " + value.toString(), null );
        }
    }

    private static int asInt( AnyValue value )
    {
        return (int) asLong( value );
    }

    private static CypherTypeException needsNumbers( String method )
    {
        return new CypherTypeException( format( "%s requires numbers", method ), null );
    }

    private static CypherTypeException notAString( String method, AnyValue in )
    {
        return new CypherTypeException(
                format( "Expected a string value for `%s`, but got: %s; consider converting it to a string with " +
                        "toString().",
                        method, in ), null );
    }
}
