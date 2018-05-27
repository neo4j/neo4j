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

import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.internal.kernel.api.CursorFactory;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.PropertyCursor;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;
import org.neo4j.internal.kernel.api.Transaction;
import org.neo4j.values.AnyValue;
import org.neo4j.values.SequenceValue;
import org.neo4j.values.ValueMapper;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.DateTimeValue;
import org.neo4j.values.storable.DateValue;
import org.neo4j.values.storable.DoubleValue;
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.IntegralValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.NumberValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TemporalValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.doubleValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

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

    public static Value sin( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.sin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "sin requires numbers", null );
        }
    }

    public static Value asin( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.asin( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "asin requires numbers", null );
        }
    }

    public static Value haversin( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( (1.0 - Math.cos( ((NumberValue) in).doubleValue() ) )/ 2 );
        }
        else
        {
            throw new CypherTypeException( "asin requires numbers", null );
        }
    }

    public static Value cos( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.cos( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "cos requires numbers", null );
        }
    }

    public static Value cot( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( 1.0 / Math.tan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "cot requires numbers", null );
        }
    }

    public static Value acos( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.acos( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "acos requires numbers", null );
        }
    }

    public static Value tan( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.tan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "tan requires numbers", null );
        }
    }

    public static Value atan( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.atan( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "atan requires numbers", null );
        }
    }

    public static Value round( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
        {
            return doubleValue( Math.round( ((NumberValue) in).doubleValue() ) );
        }
        else
        {
            throw new CypherTypeException( "round requires numbers", null );
        }
    }

    public static Value abs( AnyValue in )
    {
        if ( in == NO_VALUE )
        {
            return NO_VALUE;
        }
        else if ( in instanceof NumberValue )
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
            throw new CypherTypeException( "abs requires numbers", null );
        }
    }

    public static DoubleValue rand()
    {
        return doubleValue( ThreadLocalRandom.current().nextDouble() );
    }
}
