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
package org.neo4j.internal.collector;

import java.util.function.Supplier;

import org.neo4j.graphdb.ExecutionPlanDescription;
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
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.storable.TimeValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

/**
 * Variant of QuerySnapshot that truncates queryText and queryParameter data to limit the memory footprint of
 * constant query collection. This is crucial to avoid bloating memory use for data import scenarios, and in general
 * to avoid hogging lot's of memory that will be long-lived and likely tenured.
 */
class TruncatedQuerySnapshot
{
    final int fullQueryTextHash;
    final String queryText;
    final Supplier<ExecutionPlanDescription> queryPlanSupplier;
    final MapValue queryParameters;
    final Long elapsedTimeMicros;
    final Long compilationTimeMicros;
    final Long startTimestampMillis;

    TruncatedQuerySnapshot( String fullQueryText,
                            Supplier<ExecutionPlanDescription> queryPlanSupplier,
                            MapValue queryParameters,
                            Long elapsedTimeMicros,
                            Long compilationTimeMicros,
                            Long startTimestampMillis,
                            int maxQueryTextLength )
    {
        this.fullQueryTextHash = fullQueryText.hashCode();
        this.queryText = truncateQueryText( fullQueryText, maxQueryTextLength );
        this.queryPlanSupplier = queryPlanSupplier;
        this.queryParameters = truncateParameters( queryParameters );
        this.elapsedTimeMicros = elapsedTimeMicros;
        this.compilationTimeMicros = compilationTimeMicros;
        this.startTimestampMillis = startTimestampMillis;
    }

    private static String truncateQueryText( String queryText, int maxLength )
    {
        return queryText.length() > maxLength ? queryText.substring( 0, maxLength ) : queryText;
    }

    private static MapValue truncateParameters( MapValue parameters )
    {
        String[] keys = new String[parameters.size()];
        AnyValue[] values = new AnyValue[keys.length];

        int i = 0;
        for ( String key : parameters.keySet() )
        {
            keys[i] = key.length() <= MAX_PARAMETER_KEY_LENGTH ? key : key.substring( 0, MAX_PARAMETER_KEY_LENGTH );
            values[i] = parameters.get( key ).map( VALUE_TRUNCATER );
            i++;
        }

        return VirtualValues.map( keys, values );
    }

    private static ValueTruncater VALUE_TRUNCATER = new ValueTruncater();
    private static int MAX_TEXT_PARAMETER_LENGTH = 100;
    private static int MAX_PARAMETER_KEY_LENGTH = 1000;

    static class ValueTruncater implements ValueMapper<AnyValue>
    {

        @Override
        public AnyValue mapPath( PathValue value )
        {
            return Values.stringValue( "§PATH[" + value.size() + "]" );
        }

        @Override
        public AnyValue mapNode( VirtualNodeValue value )
        {
            if ( value instanceof NodeValue )
            {
                // Note: we do not want to keep a reference to the whole node value as it could contain a lot of data.
                return VirtualValues.node( value.id() );
            }
            return value;
        }

        @Override
        public AnyValue mapRelationship( VirtualRelationshipValue value )
        {
            if ( value instanceof RelationshipValue )
            {
                // Note: we do not want to keep a reference to the whole relationship value as it could contain a lot of data.
                return VirtualValues.relationship( value.id() );
            }
            return value;
        }

        @Override
        public AnyValue mapMap( MapValue map )
        {
            return Values.stringValue( "§MAP[" + map.size() + "]" );
        }

        @Override
        public AnyValue mapNoValue()
        {
            return Values.NO_VALUE;
        }

        @Override
        public AnyValue mapSequence( SequenceValue value )
        {
            return Values.stringValue( "§LIST[" + value.length() + "]" );
        }

        @Override
        public AnyValue mapText( TextValue value )
        {
            if ( value.length() > MAX_TEXT_PARAMETER_LENGTH )
            {
                return Values.stringValue( value.stringValue().substring( 0, MAX_TEXT_PARAMETER_LENGTH ) );
            }
            return value;
        }

        @Override
        public AnyValue mapBoolean( BooleanValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapNumber( NumberValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapDateTime( DateTimeValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapLocalDateTime( LocalDateTimeValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapDate( DateValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapTime( TimeValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapLocalTime( LocalTimeValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapDuration( DurationValue value )
        {
            return value;
        }

        @Override
        public AnyValue mapPoint( PointValue value )
        {
            return value;
        }
    }
}
