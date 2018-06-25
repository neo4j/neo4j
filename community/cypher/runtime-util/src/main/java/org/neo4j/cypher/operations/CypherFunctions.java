/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.opencypher.v9_0.util.CypherTypeException;
import org.opencypher.v9_0.util.InvalidArgumentException;
import org.opencypher.v9_0.util.ParameterWrongTypeException;

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static java.lang.String.format;
import static org.neo4j.values.storable.Values.FALSE;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.TRUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;

/**
 * This class contains static helper methods for the set of Cypher functions
 */
@SuppressWarnings( "unused" )
public final class CypherFunctions
{
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
            return dbAccess.nodeHasProperty( ((VirtualNodeValue) container).id(), key ) ? TRUE : FALSE;
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipHasProperty( ((VirtualRelationshipValue) container).id(), key ) ? TRUE : FALSE;
        }
        else if ( container instanceof MapValue )
        {
            return ((MapValue) container).containsKey( key ) ? TRUE : FALSE;
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a property container", container), null );
        }
    }

    public static AnyValue containerIndex( AnyValue container, AnyValue index, DbAccess dbAccess )
    {
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeProperty( ((VirtualNodeValue) container).id(), asString( index ) );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipProperty( ((VirtualRelationshipValue) container).id(), asString( index ) );
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

    private static AnyValue listAccess( SequenceValue container, AnyValue index )
    {
        if ( !(index instanceof IntegralValue) )
        {
            throw new CypherTypeException( format( "Expected %s to be an integer", index), null );
        }
        long idx = ((IntegralValue) index).longValue();
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

    private static String asString( AnyValue value )
    {
        if ( !(value instanceof TextValue) )
        {
            throw new CypherTypeException( format( "Expected %s to be an index key", value), null );
        }
        return ((TextValue) value).stringValue();
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
        return  new CypherTypeException(
                format("Expected a string value for `%s`, but got: %s; consider converting it to a string with toString().",
                        method, in), null);
    }


}
