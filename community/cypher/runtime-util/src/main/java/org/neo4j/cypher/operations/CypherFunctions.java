/*
 * Copyright (c) "Neo4j"
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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import org.neo4j.cypher.internal.runtime.DbAccess;
import org.neo4j.cypher.internal.runtime.ExpressionCursors;
import org.neo4j.exceptions.CypherTypeException;
import org.neo4j.exceptions.InvalidArgumentException;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.kernel.api.StatementConstants;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.token.api.TokenConstants;
import org.neo4j.util.CalledFromGeneratedCode;
import org.neo4j.values.AnyValue;
import org.neo4j.values.Equality;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.FloatingPointArray;
import org.neo4j.values.storable.IntegralArray;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.ListValueBuilder;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.MapValueBuilder;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualPathValue;
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
@SuppressWarnings( {"ReferenceEquality"} )
public final class CypherFunctions
{
    private static final BigDecimal MAX_LONG = BigDecimal.valueOf( Long.MAX_VALUE );
    private static final BigDecimal MIN_LONG = BigDecimal.valueOf( Long.MIN_VALUE );
    private static final String[] POINT_KEYS = new String[]{"crs", "x", "y", "z", "longitude", "latitude", "height", "srid"};

    private CypherFunctions()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static DoubleValue sin( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert y != NO_VALUE && x != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.floor( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "floor()" );
        }
    }

    @CalledFromGeneratedCode
    public static DoubleValue round( AnyValue in )
    {
        return round( in, Values.ZERO_INT, Values.stringValue( "HALF_UP" ), Values.booleanValue(false) );
    }

    @CalledFromGeneratedCode
    public static DoubleValue round( AnyValue in, AnyValue precision )
    {
        return round( in, precision, Values.stringValue( "HALF_UP" ), Values.booleanValue(false) );
    }

    public static DoubleValue round( AnyValue in, AnyValue precisionValue, AnyValue modeValue )
    {
        return round(in, precisionValue, modeValue, Values.booleanValue(true));
    }

    public static DoubleValue round( AnyValue in, AnyValue precisionValue, AnyValue modeValue, AnyValue explicitModeValue )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        assert precisionValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        if ( !(modeValue instanceof StringValue) )
        {
            throw notAModeString( "round", modeValue );
        }

        RoundingMode mode;
        try
        {
            mode = RoundingMode.valueOf( ((StringValue) modeValue).stringValue() );
        }
        catch ( IllegalArgumentException e )
        {
            throw new InvalidArgumentException(
                    "Unknown rounding mode. Valid values are: CEILING, FLOOR, UP, DOWN, HALF_EVEN, HALF_UP, HALF_DOWN, UNNECESSARY." );
        }

        if ( in instanceof NumberValue && precisionValue instanceof NumberValue )
        {
            int precision = asInt( precisionValue, () -> "Invalid input for precision value in function 'round()'" );
            boolean explicitMode = ((BooleanValue) explicitModeValue).booleanValue();

            if ( precision < 0 )
            {
                throw new InvalidArgumentException( "Precision argument to 'round()' cannot be negative" );
            }
            /*
             * For precision zero and no explicit rounding mode, we want to fall back to Java Math.round().
             * This rounds towards the nearest integer and if there is a tie, towards positive infinity,
             * which doesn't correspond to any of the rounding modes.
             */
            else if ( precision == 0 && !explicitMode )
            {
                return doubleValue( Math.round( ((NumberValue) in).doubleValue() ) );
            }
            else
            {
                BigDecimal bigDecimal = BigDecimal.valueOf( ((NumberValue) in).doubleValue() );
                int newScale = Math.min( bigDecimal.scale(), precision );
                return doubleValue( bigDecimal.setScale( newScale, mode ).doubleValue() );
            }
        }
        else
        {
            throw needsNumbers( "round()" );
        }
    }

    public static NumberValue abs( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof NumberValue )
        {
            return doubleValue( Math.toRadians( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw needsNumbers( "toRadians()" );
        }
    }

    @CalledFromGeneratedCode
    public static ListValue range( AnyValue startValue, AnyValue endValue )
    {
        return VirtualValues.range(
                asLong( startValue, () -> "Invalid input for start value in function 'range()'" ),
                asLong( endValue, () -> "Invalid input for end value in function 'range()'" ),
                1L );
    }

    public static ListValue range( AnyValue startValue, AnyValue endValue, AnyValue stepValue )
    {
        long step = asLong( stepValue, () -> "Invalid input for step value in function 'range()'" );
        if ( step == 0L )
        {
            throw new InvalidArgumentException( "Step argument to 'range()' cannot be zero" );
        }

        return VirtualValues.range( asLong( startValue, () -> "Invalid input for start value in function 'range()'" ),
                                    asLong( endValue, () -> "Invalid input for end value in function 'range()'" ),
                                    step );
    }

    @CalledFromGeneratedCode
    public static LongValue signum( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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

    public static TextValue randomUuid()
    {
        return stringValue( UUID.randomUUID().toString() );
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

    public static Value withinBBox( AnyValue point, AnyValue lowerLeft, AnyValue upperRight )
    {
        if ( point instanceof PointValue && lowerLeft instanceof PointValue && upperRight instanceof PointValue )
        {
            return withinBBox( (PointValue) point, (PointValue) lowerLeft, (PointValue) upperRight );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static Value withinBBox( PointValue point, PointValue lowerLeft, PointValue upperRight )
    {
        CoordinateReferenceSystem crs = point.getCoordinateReferenceSystem();
        if ( crs.equals( lowerLeft.getCoordinateReferenceSystem() ) && crs.equals( upperRight.getCoordinateReferenceSystem() ) )
        {
            return Values.booleanValue( crs.getCalculator().withinBBox( point, lowerLeft, upperRight ) );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static VirtualNodeValue startNode( AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor )
    {
        assert anyValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( anyValue instanceof RelationshipValue )
        {
            return ((RelationshipValue) anyValue).startNode();
        }
        else if ( anyValue instanceof VirtualRelationshipValue )
        {
            return startNode( (VirtualRelationshipValue) anyValue, access, cursor );
        }
        else
        {
            throw new CypherTypeException( format( "Invalid input for function 'startNode()': Expected %s to be a RelationshipValue", anyValue ) );
        }
    }

    public static VirtualNodeValue startNode( VirtualRelationshipValue relationship, DbAccess access,
                                              RelationshipScanCursor cursor )
    {
        return VirtualValues.node(
                relationship.startNodeId( relationshipVisitor ->
                                          {
                                              access.singleRelationship( relationshipVisitor.id(), cursor );
                                              if ( cursor.next() )
                                              {
                                                  relationshipVisitor.visit( cursor.sourceNodeReference(), cursor.targetNodeReference(),
                                                                             cursor.type() );
                                              }
                                          }
                )
        );
    }

    public static VirtualNodeValue endNode( AnyValue anyValue, DbAccess access, RelationshipScanCursor cursor )
    {
        assert anyValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( anyValue instanceof RelationshipValue )
        {
            return ((RelationshipValue) anyValue).endNode();
        }
        else if ( anyValue instanceof VirtualRelationshipValue )
        {
            return endNode( (VirtualRelationshipValue) anyValue, access, cursor );
        }
        else
        {
            throw new CypherTypeException( format( "Invalid input for function 'endNode()': Expected %s to be a RelationshipValue", anyValue ) );
        }
    }

    public static VirtualNodeValue endNode( VirtualRelationshipValue relationship, DbAccess access,
                                            RelationshipScanCursor cursor )
    {
        return VirtualValues.node(
                relationship.endNodeId( relationshipVisitor ->
                                        {
                                            access.singleRelationship( relationshipVisitor.id(), cursor );
                                            if ( cursor.next() )
                                            {
                                                relationshipVisitor.visit( cursor.sourceNodeReference(), cursor.targetNodeReference(),
                                                                           cursor.type() );
                                            }
                                        }
                )
        );
    }

    @CalledFromGeneratedCode
    public static VirtualNodeValue otherNode( AnyValue anyValue, DbAccess access, VirtualNodeValue node, RelationshipScanCursor cursor )
    {
        // This is not a function exposed to the user
        assert anyValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( anyValue instanceof VirtualRelationshipValue )
        {
            return otherNode( (VirtualRelationshipValue) anyValue, access, node, cursor );
        }
        else
        {
            throw new CypherTypeException( format( "Expected %s to be a RelationshipValue", anyValue ) );
        }
    }

    public static VirtualNodeValue otherNode( VirtualRelationshipValue relationship, DbAccess access, VirtualNodeValue node,
                                              RelationshipScanCursor cursor )
    {
        return VirtualValues.node( relationship.otherNodeId( node.id(), relationshipVisitor ->
        {
            access.singleRelationship( relationshipVisitor.id(), cursor );
            if ( cursor.next() )
            {
                relationshipVisitor.visit( cursor.sourceNodeReference(), cursor.targetNodeReference(), cursor.type() );
            }
        } ) );
    }

    @CalledFromGeneratedCode
    public static BooleanValue propertyExists( String key,
                                               AnyValue container,
                                               DbAccess dbAccess,
                                               NodeCursor nodeCursor,
                                               RelationshipScanCursor relationshipScanCursor,
                                               PropertyCursor propertyCursor )
    {
        assert container != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeHasProperty( ((VirtualNodeValue) container).id(), dbAccess.propertyKey( key ),
                                             nodeCursor, propertyCursor ) ? TRUE : FALSE;
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipHasProperty( ((VirtualRelationshipValue) container).id(),
                                                     dbAccess.propertyKey( key ), relationshipScanCursor,
                                                     propertyCursor ) ? TRUE : FALSE;
        }
        else if ( container instanceof MapValue )
        {
            return ((MapValue) container).get( key ) != NO_VALUE ? TRUE : FALSE;
        }
        else
        {
            throw new CypherTypeException( format( "Invalid input for function 'exists()': Expected %s to be a node, relationship or map", container ) );
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue propertyGet( String key,
                                        AnyValue container,
                                        DbAccess dbAccess,
                                        NodeCursor nodeCursor,
                                        RelationshipScanCursor relationshipScanCursor,
                                        PropertyCursor propertyCursor )
    {
        assert container != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeProperty( ((VirtualNodeValue) container).id(),
                                          dbAccess.propertyKey( key ),
                                          nodeCursor,
                                          propertyCursor,
                                          true );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipProperty( ((VirtualRelationshipValue) container).id(),
                                                  dbAccess.propertyKey( key ),
                                                  relationshipScanCursor,
                                                  propertyCursor,
                                                  true );
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
            return ((PointValue) container).get( key );
        }
        else
        {
            throw new CypherTypeException( format( "Type mismatch: expected a map but was %s", container ) );
        }
    }

    public static AnyValue containerIndex( AnyValue container,
                                           AnyValue index,
                                           DbAccess dbAccess,
                                           NodeCursor nodeCursor,
                                           RelationshipScanCursor relationshipScanCursor,
                                           PropertyCursor propertyCursor )
    {
        assert container != NO_VALUE && index != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeProperty( ((VirtualNodeValue) container).id(),
                                          propertyKeyId( dbAccess, index ),
                                          nodeCursor,
                                          propertyCursor,
                                          true );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipProperty( ((VirtualRelationshipValue) container).id(),
                                                  propertyKeyId( dbAccess, index ),
                                                  relationshipScanCursor,
                                                  propertyCursor,
                                                  true );
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
                    container, container, index ) );
        }
    }

    @CalledFromGeneratedCode
    public static boolean containerIndexExists( AnyValue container,
                                                AnyValue index,
                                                DbAccess dbAccess,
                                                NodeCursor nodeCursor,
                                                RelationshipScanCursor relationshipScanCursor,
                                                PropertyCursor propertyCursor )
    {
        assert container != NO_VALUE && index != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( container instanceof VirtualNodeValue )
        {
            return dbAccess.nodeHasProperty( ((VirtualNodeValue) container).id(),
                                             propertyKeyId( dbAccess, index ),
                                             nodeCursor, propertyCursor );
        }
        else if ( container instanceof VirtualRelationshipValue )
        {
            return dbAccess.relationshipHasProperty( ((VirtualRelationshipValue) container).id(),
                                                     propertyKeyId( dbAccess, index ),
                                                     relationshipScanCursor, propertyCursor );
        }
        if ( container instanceof MapValue )
        {
            return ((MapValue) container).containsKey( asString( index, () ->
                    // this string assumes that the asString method fails and gives context which operation went wrong
                    "Cannot use non string value as or in map keys. It was " + index.toString() ) );
        }
        else
        {
            throw new CypherTypeException( format(
                    "`%s` is not a map. Element access is only possible by performing a collection " +
                    "lookup by performing a map lookup using a string key (found: %s[%s])",
                    container, container, index ) );
        }
    }

    @CalledFromGeneratedCode
    public static AnyValue head( AnyValue container )
    {
        assert container != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
            throw new CypherTypeException( format( "Invalid input for function 'head()': Expected %s to be a list", container ) );
        }
    }

    @CalledFromGeneratedCode
    public static ListValue tail( AnyValue container )
    {
        assert container != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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

    @CalledFromGeneratedCode
    public static AnyValue last( AnyValue container )
    {
        assert container != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
            throw new CypherTypeException( format( "Invalid input for function 'last()': Expected %s to be a list", container ) );
        }
    }

    public static TextValue left( AnyValue in, AnyValue endPos )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof TextValue )
        {
            int len = asInt( endPos, () -> "Invalid input for length value in function 'left()'" );
            return ((TextValue) in).substring( 0, len );
        }
        else
        {
            throw notAString( "left", in );
        }
    }

    public static TextValue ltrim( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert original != NO_VALUE && search != NO_VALUE && replaceWith != NO_VALUE : "NO_VALUE checks need to happen outside this call";

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
        assert original != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
                    "Invalid input for function 'reverse()': " +
                    "Expected a string or a list; consider converting the value to a string with toString() or creating a list."
            );
        }
    }

    public static TextValue right( AnyValue original, AnyValue length )
    {
        assert original != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;
            int len = asInt( length, () -> "Invalid input for length value in function 'right()'" );
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
        assert original != NO_VALUE && separator != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;
            if ( asText.length() == 0 )
            {
                return VirtualValues.list( EMPTY_STRING );
            }
            if ( separator instanceof ListValue )
            {
                var separators = new ArrayList<String>();
                for ( var s : (ListValue) separator )
                {
                    separators.add( asString( s ) );
                }
                return asText.split( separators );
            }
            else
            {
                return asText.split( asString( separator ) );
            }
        }
        else
        {
            throw notAString( "split", original );
        }
    }

    public static TextValue substring( AnyValue original, AnyValue start )
    {
        assert original != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;

            return asText.substring( asInt( start, () -> "Invalid input for start value in function 'substring()'" ) );
        }
        else
        {
            throw notAString( "substring", original );
        }
    }

    public static TextValue substring( AnyValue original, AnyValue start, AnyValue length )
    {
        assert original != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( original instanceof TextValue )
        {
            TextValue asText = (TextValue) original;

            return asText.substring( asInt( start, () -> "Invalid input for start value in function 'substring()'" ),
                                     asInt( length, () -> "Invalid input for length value in function 'substring()'" ) );
        }
        else
        {
            throw notAString( "substring", original );
        }
    }

    public static TextValue toLower( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
            throw new CypherTypeException( format( "Invalid input for function 'id()': Expected %s to be a node or relationship, but it was `%s`",
                                                   item, item.getTypeName() ) );
        }
    }

    public static ListValue labels( AnyValue item, DbAccess access, NodeCursor nodeCursor )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( item instanceof VirtualNodeValue )
        {
            return access.getLabelsForNode( ((VirtualNodeValue) item).id(), nodeCursor );
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'labels()': Expected a Node, got: " + item );
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasLabel( AnyValue entity, int labelToken, DbAccess access, NodeCursor nodeCursor )
    {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( entity instanceof VirtualNodeValue )
        {
            return access.isLabelSetOnNode( labelToken, ((VirtualNodeValue) entity).id(), nodeCursor );
        }
        else
        {
            throw new CypherTypeException( "Expected a Node, got: " + entity );
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasAnyLabel( AnyValue entity, int[] labels, DbAccess access, NodeCursor nodeCursor )
    {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( entity instanceof VirtualNodeValue )
        {
            return access.isAnyLabelSetOnNode( labels, ((VirtualNodeValue) entity).id(), nodeCursor );
        }
        else
        {
            throw new CypherTypeException( "Expected a Node, got: " + entity );
        }
    }

    public static AnyValue type( AnyValue item, DbAccess access, RelationshipScanCursor relCursor )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( item instanceof RelationshipValue )
        {
            return ((RelationshipValue) item).type();
        }
        else if ( item instanceof VirtualRelationshipValue )
        {
            int typeToken = ((VirtualRelationshipValue) item).relationshipTypeId( relationshipVisitor ->
                                                                                  {
                                                                                      long id = relationshipVisitor.id();
                                                                                      access.singleRelationship( id, relCursor );
                                                                                      if ( relCursor.next() ||
                                                                                           access.relationshipDeletedInThisTransaction( id ) )
                                                                                      {
                                                                                          relationshipVisitor.visit( relCursor.sourceNodeReference(),
                                                                                                                     relCursor.targetNodeReference(),
                                                                                                                     relCursor.type() );
                                                                                      }
                                                                                  } );
            if ( typeToken == TokenConstants.NO_TOKEN )
            {
                return NO_VALUE;
            }
            else
            {
                return Values.stringValue( access.relationshipTypeName( typeToken ) );
            }
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'type()': Expected a Relationship, got: " + item );
        }
    }

    @CalledFromGeneratedCode
    public static boolean hasType( AnyValue entity, int typeToken, DbAccess access, RelationshipScanCursor relCursor )
    {
        assert entity != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( entity instanceof VirtualRelationshipValue )
        {
            if ( typeToken == StatementConstants.NO_SUCH_RELATIONSHIP_TYPE )
            {
                return false;
            }
            else
            {
                int actualType = ((VirtualRelationshipValue) entity).relationshipTypeId( relationshipVisitor ->
                                                                                         {
                                                                                             access.singleRelationship( relationshipVisitor.id(), relCursor );
                                                                                             if ( relCursor.next() )
                                                                                             {
                                                                                                 relationshipVisitor.visit( relCursor.sourceNodeReference(),
                                                                                                                            relCursor.targetNodeReference(),
                                                                                                                            relCursor.type() );
                                                                                             }
                                                                                         } );
                return typeToken == actualType;
            }
        }
        else
        {
            throw new CypherTypeException( "Expected a Relationship, got: " + entity );
        }
    }

    public static ListValue nodes( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof PathValue )
        {
            return VirtualValues.list( ((PathValue) in).nodes() );
        }
        else if ( in instanceof VirtualPathValue )
        {
            long[] ids = ((VirtualPathValue) in).nodeIds();
            ListValueBuilder builder = ListValueBuilder.newListBuilder( ids.length );
            for ( long id : ids )
            {
                builder.add( VirtualValues.node( id ) );
            }
            return builder.build();
        }
        else
        {
            throw new CypherTypeException( format( "Invalid input for function 'nodes()': Expected %s to be a path", in ) );
        }
    }

    public static ListValue relationships( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof PathValue )
        {
            return VirtualValues.list( ((PathValue) in).relationships() );
        }
        else if ( in instanceof VirtualPathValue )
        {
            long[] ids = ((VirtualPathValue) in).relationshipIds();
            ListValueBuilder builder = ListValueBuilder.newListBuilder( ids.length );
            for ( long id : ids )
            {
                builder.add( VirtualValues.relationship( id ) );
            }
            return builder.build();
        }
        else
        {
            throw new CypherTypeException( format( "Invalid input for function 'relationships()': Expected %s to be a path", in ) );
        }
    }

    public static Value point( AnyValue in, DbAccess access, ExpressionCursors cursors )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof VirtualNodeValue )
        {
            return asPoint( access, (VirtualNodeValue) in, cursors.nodeCursor(), cursors.propertyCursor() );
        }
        else if ( in instanceof VirtualRelationshipValue )
        {
            return asPoint( access, (VirtualRelationshipValue) in, cursors.relationshipScanCursor(), cursors.propertyCursor() );
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
            throw new CypherTypeException( format( "Invalid input for function 'point()': Expected a map but got %s", in ) );
        }
    }

    public static ListValue keys( AnyValue in,
                                  DbAccess access,
                                  NodeCursor nodeCursor,
                                  RelationshipScanCursor relationshipScanCursor,
                                  PropertyCursor propertyCursor )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof VirtualNodeValue )
        {
            return extractKeys( access, access.nodePropertyIds( ((VirtualNodeValue) in).id(), nodeCursor, propertyCursor ) );
        }
        else if ( in instanceof VirtualRelationshipValue )
        {
            return extractKeys( access, access.relationshipPropertyIds( ((VirtualRelationshipValue) in).id(),
                                                                        relationshipScanCursor,
                                                                        propertyCursor ) );
        }
        else if ( in instanceof MapValue )
        {
            return ((MapValue) in).keys();
        }
        else
        {
            throw new CypherTypeException( format(
                    "Invalid input for function 'keys()': Expected a node, a relationship or a literal map but got %s", in ) );
        }
    }

    public static MapValue properties( AnyValue in,
                                       DbAccess access,
                                       NodeCursor nodeCursor,
                                       RelationshipScanCursor relationshipCursor,
                                       PropertyCursor propertyCursor )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof VirtualNodeValue )
        {
            return access.nodeAsMap( ((VirtualNodeValue) in).id(), nodeCursor, propertyCursor );
        }
        else if ( in instanceof VirtualRelationshipValue )
        {
            return access.relationshipAsMap( ((VirtualRelationshipValue) in).id(), relationshipCursor, propertyCursor );
        }
        else if ( in instanceof MapValue )
        {
            return (MapValue) in;
        }
        else
        {
            throw new CypherTypeException( format(
                    "Invalid input for function 'properties()': Expected a node, a relationship or a literal map but got %s", in ) );
        }
    }

    public static IntegralValue size( AnyValue item )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        if ( item instanceof TextValue )
        {
            return longValue( ((TextValue) item).length() );
        }
        else if ( item instanceof SequenceValue )
        {
            return longValue( ((SequenceValue) item).length() );
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'size()': Expected a String or List, got: " + item );
        }
    }

    public static BooleanValue isEmpty( AnyValue item )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( item instanceof SequenceValue )
        {
            return Values.booleanValue( ((SequenceValue) item).isEmpty() );
        }
        else if ( item instanceof MapValue )
        {
            return Values.booleanValue( ((MapValue) item).isEmpty() );
        }
        else if ( item instanceof TextValue )
        {
            return Values.booleanValue( ((TextValue) item).isEmpty() );
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'isEmpty()': Expected a List, Map, or String, got: " + item );
        }
    }

    public static IntegralValue length( AnyValue item )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( item instanceof VirtualPathValue )
        {
            return longValue( ((VirtualPathValue) item).size() );
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'length()': Expected a Path, got: " + item );
        }
    }

    public static IntegralValue length3_5( AnyValue item )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( item instanceof VirtualPathValue )
        {
            return length( item );
        }
        else if ( item instanceof TextValue || item instanceof SequenceValue )
        {
            return size( item );
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'length3_5()': Expected a Path, String or List, got: " + item );
        }
    }

    public static Value toBoolean( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        else if ( in instanceof IntegralValue )
        {
            return ((IntegralValue) in).longValue() == 0L ? FALSE : TRUE;
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'toBoolean()': Expected a Boolean, Integer or String, got: " + in );
        }
    }

    public static Value toBooleanOrNull( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof BooleanValue || in instanceof TextValue || in instanceof IntegralValue )
        {
            return toBoolean( in );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static AnyValue toBooleanList( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( !(in instanceof ListValue) )
        {
            throw new CypherTypeException( String.format( "Invalid input for function 'toBooleanList()': Expected a List, got: %s", in ) );
        }

        ListValue lv = (ListValue) in;

        return Arrays.stream( lv.asArray() )
                     .map( entry -> entry == NO_VALUE ? NO_VALUE : toBooleanOrNull( entry ) )
                     .collect( ListValueBuilder.collector() );
    }

    public static Value toFloat( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
            throw new CypherTypeException( "Invalid input for function 'toFloat()': Expected a String or Number, got: " + in );
        }
    }

    public static Value toFloatOrNull( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof NumberValue || in instanceof TextValue )
        {
            return toFloat( in );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static AnyValue toFloatList( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( !(in instanceof ListValue) )
        {
            throw new CypherTypeException( String.format( "Invalid input for function 'toFloatList()': Expected a List, got: %s", in ) );
        }

        ListValue lv = (ListValue) in;

        return Arrays.stream( lv.asArray() )
                     .map( entry -> entry == NO_VALUE ? NO_VALUE : toFloatOrNull( entry ) )
                     .collect( ListValueBuilder.collector() );
    }

    public static Value toInteger( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
        else if ( in instanceof BooleanValue )
        {
            if ( ((BooleanValue) in).booleanValue() )
            {
                return longValue( 1L );
            }
            else
            {
                return longValue( 0L );
            }
        }
        else
        {
            throw new CypherTypeException( "Invalid input for function 'toInteger()': Expected a String, Number or Boolean, got: " + in );
        }
    }

    public static Value toIntegerOrNull( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof NumberValue || in instanceof BooleanValue )
        {
            return toInteger( in );
        }
        else if ( in instanceof TextValue )
        {
            try
            {
                return stringToLongValue( (TextValue) in );
            }
            catch ( CypherTypeException e )
            {
                return NO_VALUE;
            }
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static ListValue toIntegerList( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof IntegralArray )
        {
            return VirtualValues.fromArray( (ArrayValue) in );
        }
        else if ( in instanceof FloatingPointArray )
        {
            return toIntegerList( (FloatingPointArray) in );
        }
        else if ( in instanceof SequenceValue )
        {
            return toIntegerList( (SequenceValue) in );
        }
        else
        {
            throw new CypherTypeException( String.format( "Invalid input for function 'toIntegerList()': Expected a List, got: %s", in ) );
        }
    }

    public static TextValue toString( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
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
            throw new CypherTypeException(
                    "Invalid input for function 'toString()': Expected a String, Number, Boolean, Temporal or Duration, got: " + in );
        }
    }

    public static AnyValue toStringOrNull( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( in instanceof TextValue || in instanceof NumberValue || in instanceof BooleanValue || in instanceof TemporalValue || in instanceof DurationValue ||
             in instanceof PointValue )
        {
            return toString( in );
        }
        else
        {
            return NO_VALUE;
        }
    }

    public static AnyValue toStringList( AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( !(in instanceof ListValue) )
        {
            throw new CypherTypeException( String.format( "Invalid input for function 'toStringList()': Expected a List, got: %s", in ) );
        }

        ListValue lv = (ListValue) in;

        return Arrays.stream( lv.asArray() )
                     .map( entry -> entry == NO_VALUE ? NO_VALUE : toStringOrNull( entry ) )
                     .collect( ListValueBuilder.collector() );
    }

    public static ListValue fromSlice( AnyValue collection, AnyValue fromValue )
    {
        assert collection != NO_VALUE && fromValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";

        int from = asInt( fromValue );
        ListValue list = asList( collection );
        if ( from >= 0 )
        {
            return list.drop( from );
        }
        else
        {
            return list.drop( list.size() + from );
        }
    }

    public static ListValue toSlice( AnyValue collection, AnyValue toValue )
    {
        assert collection != NO_VALUE && toValue != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        int from = asInt( toValue );
        ListValue list = asList( collection );
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
        assert collection != NO_VALUE && fromValue != NO_VALUE && toValue != NO_VALUE :
                "NO_VALUE checks need to happen outside this call";

        int from = asInt( fromValue );
        int to = asInt( toValue );
        ListValue list = asList( collection );
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

    public static ListValue asList( AnyValue collection )
    {
        if ( collection == NO_VALUE )
        {
            return VirtualValues.EMPTY_LIST;
        }
        else if ( collection instanceof ListValue )
        {
            return (ListValue) collection;
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

    @CalledFromGeneratedCode
    public static Value in( AnyValue findMe, AnyValue lookIn )
    {
        if ( lookIn == NO_VALUE )
        {
            return NO_VALUE;
        }

        var iterator = asList( lookIn ).iterator();

        if ( !iterator.hasNext() )
        {
            return BooleanValue.FALSE;
        }

        if ( findMe == NO_VALUE )
        {
            return NO_VALUE;
        }

        var undefinedEquality = false;
        while ( iterator.hasNext() )
        {
            var nextValue = iterator.next();
            var equality = nextValue.ternaryEquals( findMe );

            if ( equality == Equality.TRUE )
            {
                return BooleanValue.TRUE;
            }
            else if ( equality == Equality.UNDEFINED && !undefinedEquality )
            {
                undefinedEquality = true;
            }
        }

        return undefinedEquality ? NO_VALUE : BooleanValue.FALSE;
    }

    @CalledFromGeneratedCode
    public static Value in( AnyValue findMe, AnyValue lookIn, InCache cache, MemoryTracker memoryTracker )
    {
        if ( lookIn == NO_VALUE )
        {
            return NO_VALUE;
        }

        return cache.check( findMe, asList( lookIn ), memoryTracker );
    }

    @CalledFromGeneratedCode
    public static TextValue asTextValue( AnyValue value )
    {
        return asTextValue( value, null );
    }

    public static TextValue asTextValue( AnyValue value, Supplier<String> contextForErrorMessage )
    {
        if ( !(value instanceof TextValue) )
        {
            String errorMessage;
            if ( contextForErrorMessage == null )
            {
                errorMessage = format( "Expected %s to be a %s, but it was a %s",
                                       value,
                                       TextValue.class.getName(),
                                       value.getClass().getName() );
            }
            else
            {
                errorMessage = format( "%s: Expected %s to be a %s, but it was a %s",
                                       contextForErrorMessage.get(),
                                       value,
                                       TextValue.class.getName(),
                                       value.getClass().getName() );
            }

            throw new CypherTypeException( errorMessage );
        }
        return (TextValue) value;
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
                    throw new CypherTypeException( format( "integer, %s, is too large", in.stringValue() ) );
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

    private static Value asPoint( DbAccess access,
                                  VirtualNodeValue nodeValue,
                                  NodeCursor nodeCursor,
                                  PropertyCursor propertyCursor )
    {
        MapValueBuilder builder = new MapValueBuilder();
        for ( String key : POINT_KEYS )
        {
            Value value = access.nodeProperty( nodeValue.id(),
                                               access.propertyKey( key ),
                                               nodeCursor,
                                               propertyCursor,
                                               true );
            if ( value == NO_VALUE )
            {
                continue;
            }
            builder.add( key, value );
        }

        return PointValue.fromMap( builder.build() );
    }

    private static Value asPoint( DbAccess access,
                                  VirtualRelationshipValue relationshipValue,
                                  RelationshipScanCursor relationshipScanCursor,
                                  PropertyCursor propertyCursor )
    {
        MapValueBuilder builder = new MapValueBuilder();
        for ( String key : POINT_KEYS )
        {
            Value value = access.relationshipProperty( relationshipValue.id(),
                                                       access.propertyKey( key ),
                                                       relationshipScanCursor,
                                                       propertyCursor,
                                                       true );
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
        map.foreach( ( s, value ) ->
                     {
                         if ( value == NO_VALUE )
                         {
                             hasNull[0] = true;
                         }
                     } );
        return hasNull[0];
    }

    private static AnyValue listAccess( SequenceValue container, AnyValue index )
    {
        NumberValue number = asNumberValue( index,
                                            () -> "Cannot access a list '" + container.toString() + "' using a non-number index, got " + index.toString() );
        if ( !(number instanceof IntegralValue) )
        {
            throw new CypherTypeException( format( "Cannot access a list using an non-integer number index, got %s", number ),
                                           null );
        }
        long idx = number.longValue();
        if ( idx > Integer.MAX_VALUE || idx < Integer.MIN_VALUE )
        {
            throw new InvalidArgumentException(
                    format( "Cannot index a list using a value greater than %d or lesser than %d, got %d",
                            Integer.MAX_VALUE, Integer.MIN_VALUE, idx ) );
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

    private static int propertyKeyId( DbAccess dbAccess, AnyValue index )
    {
        return dbAccess.propertyKey( asString( index, () ->
                // this string assumes that the asString method fails and gives context which operation went wrong
                "Cannot use a property key with non string name. It was " + index.toString() ) );
    }

    private static AnyValue mapAccess( MapValue container, AnyValue index )
    {
        return container.get( asString( index, () ->
                // this string assumes that the asString method fails and gives context which operation went wrong
                "Cannot access a map '" + container.toString() + "' by key '" + index.toString() + "'" ) );
    }

    private static String asString( AnyValue value )
    {
        return asTextValue( value ).stringValue();
    }

    private static String asString( AnyValue value, Supplier<String> contextForErrorMessage )
    {
        return asTextValue( value, contextForErrorMessage ).stringValue();
    }

    private static NumberValue asNumberValue( AnyValue value, Supplier<String> contextForErrorMessage )
    {
        if ( !(value instanceof NumberValue) )
        {
            throw new CypherTypeException( format( "%s: Expected %s to be a %s, but it was a %s",
                                                   contextForErrorMessage.get(),
                                                   value,
                                                   NumberValue.class.getName(),
                                                   value.getClass().getName()
            ) );
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

    private static long asLong( AnyValue value, Supplier<String> contextForErrorMessage )
    {
        if ( value instanceof NumberValue )
        {
            return ((NumberValue) value).longValue();
        }
        else
        {
            String errorMsg;
            if ( contextForErrorMessage == null )
            {
                errorMsg = "Expected a numeric value but got: " + value;
            }
            else
            {
                errorMsg = contextForErrorMessage.get() + ": Expected a numeric value but got: " + value;
            }
            throw new CypherTypeException( errorMsg );
        }
    }

    @CalledFromGeneratedCode
    public static int asInt( AnyValue value )
    {
        return asInt( value, null );
    }

    public static int asInt( AnyValue value, Supplier<String> contextForErrorMessage )
    {
        return (int) asLong( value, contextForErrorMessage );
    }

    public static long nodeId( AnyValue value )
    {
        assert value != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( value instanceof VirtualNodeValue )
        {
            return ((VirtualNodeValue) value).id();
        }
        else
        {
            throw new CypherTypeException( "Expected VirtualNodeValue got " + value.getClass().getName() );
        }
    }

    public static BooleanValue assertIsNode( AnyValue item )
    {
        assert item != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        if ( item instanceof VirtualNodeValue )
        {
            return TRUE;
        }
        else
        {
            throw new CypherTypeException( "Expected a Node, got: " + item );
        }
    }

    private static CypherTypeException needsNumbers( String method )
    {
        return new CypherTypeException( format( "%s requires numbers", method ) );
    }

    private static CypherTypeException notAString( String method, AnyValue in )
    {
        assert in != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        return new CypherTypeException(
                format( "Expected a string value for `%s`, but got: %s; consider converting it to a string with " +
                        "toString().",
                        method, in ) );
    }

    private static CypherTypeException notAModeString( String method, AnyValue mode )
    {
        assert mode != NO_VALUE : "NO_VALUE checks need to happen outside this call";
        return new CypherTypeException(
                format( "Expected a string value for `%s`, but got: %s.",
                        method, mode ) );
    }

    private static ListValue toIntegerList( FloatingPointArray array )
    {
        var converted = ListValueBuilder.newListBuilder( array.length() );
        for ( int i = 0; i < array.length(); i++ )
        {
            converted.add( longValue( (long) array.doubleValue( i ) ) );
        }
        return converted.build();
    }

    private static ListValue toIntegerList( SequenceValue sequenceValue )
    {
        var converted = ListValueBuilder.newListBuilder();
        for ( AnyValue value : sequenceValue )
        {
            converted.add( value != NO_VALUE ? toIntegerOrNull( value ) : NO_VALUE );
        }
        return converted.build();
    }
}
