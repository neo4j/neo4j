/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.codegen;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.util.ValueUtils;
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
import org.neo4j.values.virtual.NodeReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

public final class CompiledMaterializeValueMapper
{
    public static AnyValue mapAnyValue( EmbeddedProxySPI proxySPI, AnyValue value )
    {
        // First do a dry run to determine if any conversion will actually be needed,
        // because if it isn't we can just return the value as it is without having
        // to recursively rewrite lists and maps, which could be very expensive.
        // This is based on the assumption that returning full nodes and relationships is a
        // relatively rare use-case compared to returning a selection of their properties.
        // Hopefully the dry run will also heat up the caches for the real run, thus reducing its overhead.
        DryRunMaterializeValueMapper dryRunMapper = new DryRunMaterializeValueMapper();
        value.map( dryRunMapper );
        if ( dryRunMapper.needsConversion )
        {
            WritingMaterializeValueMapper realMapper = new WritingMaterializeValueMapper( proxySPI );
            return value.map( realMapper );
        }
        return value;
    }

    private static final class WritingMaterializeValueMapper extends AbstractMaterializeValueMapper
    {
        private EmbeddedProxySPI proxySpi;

        WritingMaterializeValueMapper( EmbeddedProxySPI proxySpi )
        {
            this.proxySpi = proxySpi;
        }

        // Create proxy wrapping values for nodes and relationships that are not already such values

        @Override
        public AnyValue mapNode( VirtualNodeValue value )
        {
            if ( value instanceof NodeValue )
            {
                return value;
            }
            return ValueUtils.fromNodeProxy( proxySpi.newNodeProxy( value.id() ) );
        }

        @Override
        public AnyValue mapRelationship( VirtualRelationshipValue value )
        {
            if ( value instanceof RelationshipValue )
            {
                return value;
            }
            return ValueUtils.fromRelationshipProxy( proxySpi.newRelationshipProxy( value.id() ) );
        }

        // Recurse through maps and sequences

        @Override
        public AnyValue mapMap( MapValue value )
        {
            Map<String,AnyValue> map = new HashMap<>();
            value.foreach( ( k, v ) -> map.put( k, v.map( this ) ) );
            return VirtualValues.map( map );
        }

        @Override
        public AnyValue mapSequence( SequenceValue value )
        {
            List<AnyValue> list = new ArrayList<>( value.length() );
            value.forEach( v -> list.add( v.map( this ) ) );
            return VirtualValues.fromList( list );
        }
    }

    private static final class DryRunMaterializeValueMapper extends AbstractMaterializeValueMapper
    {
        boolean needsConversion;

        @Override
        public AnyValue mapNode( VirtualNodeValue value )
        {
            if ( !needsConversion )
            {
                needsConversion = value instanceof NodeReference;
            }
            return value;
        }

        @Override
        public AnyValue mapRelationship( VirtualRelationshipValue value )
        {
            if ( !needsConversion )
            {
                needsConversion = value instanceof RelationshipReference;
            }
            return value;
        }

        // Recurse through maps and sequences

        @Override
        public AnyValue mapMap( MapValue value )
        {
            value.foreach( ( k, v ) -> v.map( this ) );
            return value;
        }

        @Override
        public AnyValue mapSequence( SequenceValue value )
        {
            value.forEach( v -> v.map( this ) );
            return (AnyValue) value;
        }
    }

    abstract static class AbstractMaterializeValueMapper implements ValueMapper<AnyValue>
    {
        // Paths do not require any conversion at this point

        @Override
        public AnyValue mapPath( PathValue value )
        {
            return value;
        }

        // Preserve the scalar AnyValue types as they are

        @Override
        public AnyValue mapNoValue()
        {
            return Values.NO_VALUE;
        }

        @Override
        public AnyValue mapText( TextValue value )
        {
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
