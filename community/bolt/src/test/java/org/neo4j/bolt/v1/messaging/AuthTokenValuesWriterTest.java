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
package org.neo4j.bolt.v1.messaging;

import org.junit.Test;

import org.neo4j.values.storable.CoordinateReferenceSystem;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.PointValue;
import org.neo4j.values.storable.TextValue;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.pointValue;
import static org.neo4j.values.storable.Values.stringArray;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;
import static org.neo4j.values.virtual.VirtualValues.nodeValue;
import static org.neo4j.values.virtual.VirtualValues.relationshipValue;

public class AuthTokenValuesWriterTest
{
    @Test
    public void shouldFailToWriteNode()
    {
        AuthTokenValuesWriter writer = new AuthTokenValuesWriter();

        NodeValue value = nodeValue( 42, stringArray( "Person" ), EMPTY_MAP );

        try
        {
            writer.valueAsObject( value );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }

    @Test
    public void shouldFailToWriteRelationship()
    {
        AuthTokenValuesWriter writer = new AuthTokenValuesWriter();

        NodeValue startNode = nodeValue( 24, stringArray( "Person" ), EMPTY_MAP );
        NodeValue endNode = nodeValue( 42, stringArray( "Person" ), EMPTY_MAP );
        RelationshipValue value = relationshipValue( 42, startNode, endNode, stringValue( "KNOWS" ), EMPTY_MAP );

        try
        {
            writer.valueAsObject( value );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }

    @Test
    public void shouldFailToWritePoint()
    {
        AuthTokenValuesWriter writer = new AuthTokenValuesWriter();
        PointValue value = pointValue( CoordinateReferenceSystem.WGS84, new double[2] );

        try
        {
            writer.valueAsObject( value );
            fail( "Exception expected" );
        }
        catch ( UnsupportedOperationException ignore )
        {
        }
    }

    @Test
    public void shouldConvertStringValueToString()
    {
        AuthTokenValuesWriter writer = new AuthTokenValuesWriter();
        TextValue value = stringValue( "Hello" );

        assertEquals( "Hello", writer.valueAsObject( value ) );
    }

    @Test
    public void shouldConvertLongValueToLong()
    {
        AuthTokenValuesWriter writer = new AuthTokenValuesWriter();
        LongValue value = longValue( 42 );

        assertEquals( 42L, writer.valueAsObject( value ) );
    }

    @Test
    public void shouldConvertMultipleValues()
    {
        AuthTokenValuesWriter writer = new AuthTokenValuesWriter();

        TextValue value1 = stringValue( "Hello" );
        TextValue value2 = stringValue( " " );
        TextValue value3 = stringValue( "World!" );
        LongValue value4 = longValue( 42 );

        assertEquals( "Hello", writer.valueAsObject( value1 ) );
        assertEquals( " ", writer.valueAsObject( value2 ) );
        assertEquals( "World!", writer.valueAsObject( value3 ) );
        assertEquals( 42L, writer.valueAsObject( value4 ) );
    }
}
