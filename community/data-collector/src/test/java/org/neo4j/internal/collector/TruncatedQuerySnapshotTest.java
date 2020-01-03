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

import org.junit.jupiter.api.Test;

import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TruncatedQuerySnapshotTest
{
    static NodeValue NODE = VirtualValues.nodeValue( 42,
                                                     Values.stringArray( "Phone" ),
                                                     map( "number", Values.stringValue( "07303725xx" ) ) );

    static RelationshipValue RELATIONSHIP = VirtualValues.relationshipValue( 100,
                                                                             NODE,
                                                                             NODE,
                                                                             Values.stringValue( "CALL" ),
                                                                             map( "duration", Values.stringValue( "3 hours" ) ) );

    @Test
    void shouldTruncateNode()
    {
        // when
        TruncatedQuerySnapshot x = new TruncatedQuerySnapshot( "",
                                                               null,
                                                               map( "n", NODE ),
                                                               -1L,
                                                               -1L,
                                                               -1L,
                                                               100 );

        // then
        AnyValue truncatedNode = x.queryParameters.get( "n" );
        assertTrue( truncatedNode instanceof NodeReference );
        assertEquals( NODE.id(), ((NodeReference)truncatedNode).id() );
    }

    @Test
    void shouldTruncateRelationship()
    {
        // when
        TruncatedQuerySnapshot x = new TruncatedQuerySnapshot( "",
                                                               null,
                                                               map( "r", RELATIONSHIP ),
                                                               -1L,
                                                               -1L,
                                                               -1L,
                                                               100 );

        // then
        AnyValue truncatedRelationship = x.queryParameters.get( "r" );
        assertTrue( truncatedRelationship instanceof RelationshipReference );
        assertEquals( RELATIONSHIP.id(), ((RelationshipReference)truncatedRelationship).id() );
    }

    private static MapValue map( String key, AnyValue value )
    {
        String[] keys = {key};
        AnyValue[] values = {value};
        return VirtualValues.map( keys, values );
    }
}
