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
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.opencypher.v9_0.util.CypherTypeException;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.ArrayValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.StringValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This class contains static helper methods used by the compiled expressions
 */
public final class ExpressionMethods
{
    private ExpressionMethods()
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
            throw new CypherTypeException( "sin requires numbers", null );
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
            throw new CypherTypeException( "round requires numbers", null );
        }
    }

    public static DoubleValue rand()
    {
        return doubleValue( ThreadLocalRandom.current().nextDouble() );
    }

    //TODO this is horrible spaghetti code, we should push most of this down to AnyValue
    public static AnyValue add( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs == NO_VALUE || rhs == NO_VALUE )
        {
            return NO_VALUE;
        }
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).plus( (NumberValue) rhs );
        }
        //List addition
        boolean lhsIsListValue = lhs instanceof ListValue;
        if ( lhsIsListValue && rhs instanceof ListValue )
        {
            return VirtualValues.concat( (ListValue) lhs, (ListValue) rhs );
        }
        else if ( lhsIsListValue )
        {
            return VirtualValues.appendToList( (ListValue) lhs, rhs );
        }
        else if ( rhs instanceof ListValue )
        {
            return VirtualValues.prependToList( (ListValue) rhs, lhs );
        }

        // String addition
        if ( lhs instanceof TextValue )
        {
            if ( rhs instanceof Value )
            {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to
                // exclude them
                if ( !(rhs instanceof TemporalValue || rhs instanceof DurationValue || rhs instanceof PointValue) )
                {
                    return stringValue( ((TextValue) lhs).stringValue() + ((Value) rhs).prettyPrint());
                }
                else
                {
                    //TODO this seems wrong but it is what we currently do in compiled runtime
                    return stringValue(((TextValue) lhs).stringValue() + String.valueOf( rhs ));
                }
            }
        }
        if ( rhs instanceof TextValue )
        {
            if ( lhs instanceof Value )
            {
                // Unfortunately string concatenation is not defined for temporal and spatial types, so we need to
                // exclude them
                if ( !(lhs instanceof TemporalValue || lhs instanceof DurationValue || lhs instanceof PointValue) )
                {
                    return stringValue( ((Value) lhs).prettyPrint() + ((TextValue) rhs).stringValue());
                }
                else
                {
                    //TODO this seems wrong but it is what we currently do in compiled runtime
                    return stringValue(String.valueOf( lhs ) + ((TextValue) rhs).stringValue() );
                }
            }
        }

        // Temporal values
        if ( lhs instanceof TemporalValue )
        {
            if ( rhs instanceof DurationValue )
            {
                return ((TemporalValue) lhs).plus( (DurationValue) rhs );
            }
        }
        if ( lhs instanceof DurationValue )
        {
            if ( rhs instanceof TemporalValue )
            {
                return ((TemporalValue) rhs).plus( (DurationValue) lhs );
            }
            if ( rhs instanceof DurationValue )
            {
                return ((DurationValue) lhs).add( (DurationValue) rhs );
            }
        }

        throw new CypherTypeException(
                String.format( "Don't know how to add `%s` and `%s`", lhs, rhs), null );
    }

    public static AnyValue subtract( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).minus( (NumberValue) rhs );
        }
        else
        {
            //todo
            throw new CypherTypeException( "can only subtract numbers", null );
        }
    }

    public static AnyValue multiply( AnyValue lhs, AnyValue rhs )
    {
        if ( lhs instanceof NumberValue && rhs instanceof NumberValue )
        {
            return ((NumberValue) lhs).times( (NumberValue) rhs );
        }
        else
        {
            //todo
            throw new CypherTypeException( "can only multiply numbers", null );
        }
    }

    public static Value nodeProperty( Transaction tx, long node, int property )
    {
        CursorFactory cursors = tx.cursors();
        try ( NodeCursor nodes = cursors.allocateNodeCursor();
              PropertyCursor properties = cursors.allocatePropertyCursor() )
        {
            tx.dataRead().singleNode( node, nodes );
            if ( nodes.next() )
            {
                nodes.properties( properties );
                while ( properties.next() )
                {
                    if ( properties.propertyKey() == property )
                    {
                        return properties.propertyValue();
                    }
                }
            }
            return Values.NO_VALUE;
        }
    }
}
