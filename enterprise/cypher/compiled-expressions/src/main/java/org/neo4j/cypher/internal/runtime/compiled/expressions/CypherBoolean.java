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
import org.neo4j.values.storable.DurationValue;
import org.neo4j.values.storable.LocalDateTimeValue;
import org.neo4j.values.storable.LocalTimeValue;
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
import static org.neo4j.values.storable.Values.stringValue;

/**
 * This class contains static helper math methods used by the compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CypherBoolean
{
    private static final BooleanMapper BOOLEAN_MAPPER = new BooleanMapper();

    private CypherBoolean()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static Value or( AnyValue... args )
    {
        for ( AnyValue arg : args )
        {
            if ( arg == NO_VALUE )
            {
                return NO_VALUE;
            }

            if ( arg.map( BOOLEAN_MAPPER ) )
            {
                return Values.TRUE;
            }
        }
        return Values.FALSE;
    }

    private static final class BooleanMapper implements ValueMapper<Boolean>
    {
        @Override
        public Boolean mapPath( PathValue value )
        {
            return value.size() > 0;
        }

        @Override
        public Boolean mapNode( VirtualNodeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapRelationship( VirtualRelationshipValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapMap( MapValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapNoValue()
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + NO_VALUE, null );
        }

        @Override
        public Boolean mapSequence( SequenceValue value )
        {
            return value.length() > 0;
        }

        @Override
        public Boolean mapText( TextValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapBoolean( BooleanValue value )
        {
            return value.booleanValue();
        }

        @Override
        public Boolean mapNumber( NumberValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapDateTime( DateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapLocalDateTime( LocalDateTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapDate( DateValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapTime( TimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapLocalTime( LocalTimeValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapDuration( DurationValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }

        @Override
        public Boolean mapPoint( PointValue value )
        {
            throw new CypherTypeException( "Don't know how to treat that as a boolean: " + value, null );
        }
    }
}
