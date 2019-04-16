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
package org.neo4j.cypher.internal.runtime;

import org.neo4j.kernel.impl.util.NodeProxyWrappingNodeValue;
import org.neo4j.kernel.impl.util.RelationshipProxyWrappingValue;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.RelationshipValue;

public final class ValuePopulation
{
    private ValuePopulation()
    {
        throw new UnsupportedOperationException( "Do not instantiate" );
    }

    public static AnyValue populate( AnyValue value )
    {
        if ( value instanceof NodeProxyWrappingNodeValue )
        {
            ((NodeProxyWrappingNodeValue) value).populate();
        }
        else if ( value instanceof RelationshipProxyWrappingValue )
        {
            ((RelationshipProxyWrappingValue) value).populate();
        }
        else if ( value instanceof PathValue )
        {
            PathValue path = (PathValue) value;
            for ( NodeValue node : path.nodes() )
            {
                populate( node );
            }
            for ( RelationshipValue relationship : path.relationships() )
            {
                populate( relationship );
            }
        }
        else if ( value instanceof ListValue )
        {
            for ( AnyValue v : (ListValue) value )
            {
                populate( v );
            }
        }
        else if ( value instanceof MapValue )
        {
            ((MapValue) value).foreach( ( ignore, anyValue ) -> populate( anyValue ) );
        }

        return value;
    }

    public static NodeValue populate( NodeValue value )
    {
        if ( value instanceof NodeProxyWrappingNodeValue )
        {
            ((NodeProxyWrappingNodeValue) value).populate();
        }
        return value;
    }

    public static RelationshipValue populate( RelationshipValue value )
    {
        if ( value instanceof RelationshipProxyWrappingValue )
        {
            ((RelationshipProxyWrappingValue) value).populate();
        }
        return value;
    }
}
