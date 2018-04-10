/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.NodeProxyWrappingNodeValue;
import org.neo4j.kernel.impl.util.PathWrappingPathValue;
import org.neo4j.kernel.impl.util.RelationshipProxyWrappingValue;
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
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualNodeValue;
import org.neo4j.values.virtual.VirtualRelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

public final class CompiledMaterializeValueMapper implements ValueMapper<AnyValue>
{
    private DefaultValueMapper defaultValueMapper;

    public CompiledMaterializeValueMapper( EmbeddedProxySPI proxySPI )
    {
        defaultValueMapper = new DefaultValueMapper( proxySPI );
    }

    public static AnyValue mapAnyValue( EmbeddedProxySPI proxySPI, AnyValue value )
    {
        CompiledMaterializeValueMapper mapper = new CompiledMaterializeValueMapper( proxySPI );
        return value.map( mapper );
    }

    // Create proxy wrapping values for nodes, relationships and paths that are not already such values

    @Override
    public AnyValue mapNode( VirtualNodeValue value )
    {
        if ( value instanceof NodeProxyWrappingNodeValue )
        {
            return value;
        }
        return ValueUtils.fromNodeProxy( defaultValueMapper.mapNode( value ) );
    }

    @Override
    public AnyValue mapRelationship( VirtualRelationshipValue value )
    {
        if ( value instanceof RelationshipProxyWrappingValue )
        {
            return value;
        }
        return ValueUtils.fromRelationshipProxy( defaultValueMapper.mapRelationship( value ) );
    }

    @Override
    public AnyValue mapPath( PathValue value )
    {
        if ( value instanceof PathWrappingPathValue )
        {
            return value;
        }
        return ValueUtils.fromPath( defaultValueMapper.mapPath( value ) );
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
