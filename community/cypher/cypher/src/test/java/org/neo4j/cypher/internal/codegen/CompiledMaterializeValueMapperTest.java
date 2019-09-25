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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.core.NodeEntity;
import org.neo4j.kernel.impl.core.RelationshipEntity;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.Values;
import org.neo4j.values.virtual.ListValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.NodeReference;
import org.neo4j.values.virtual.NodeValue;
import org.neo4j.values.virtual.RelationshipReference;
import org.neo4j.values.virtual.RelationshipValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class CompiledMaterializeValueMapperTest
{
    private static final InternalTransaction transaction = mock( InternalTransaction.class );

    private static final NodeValue nodeEntityValue = ValueUtils.fromNodeEntity( new NodeEntity( transaction, 1L ) );
    private static final NodeValue directNodeValue = VirtualValues.nodeValue( 2L, Values.stringArray(), VirtualValues.EMPTY_MAP );
    private static final NodeReference nodeReference = VirtualValues.node( 1L ); // Should equal nodeEntityValue when converted

    private static final RelationshipValue relationshipEntityValue = ValueUtils.fromRelationshipEntity( new RelationshipEntity( transaction, 11L ) );
    private static final RelationshipValue directRelationshipValue =
            VirtualValues.relationshipValue( 12L, nodeEntityValue, directNodeValue, Values.stringValue( "TYPE" ), VirtualValues.EMPTY_MAP );
    private static final RelationshipReference relationshipReference = VirtualValues.relationship( 11L ); // Should equal relationshipEntityValue when converted

    @BeforeEach
    void setUp()
    {
        when( transaction.newNodeEntity( anyLong() ) )
                .thenAnswer( (Answer<NodeEntity>) invocation -> new NodeEntity( transaction, invocation.getArgument( 0 ) ) );
        when( transaction.newRelationshipEntity( anyLong() ) )
                .thenAnswer( (Answer<RelationshipEntity>) invocation ->
                        new RelationshipEntity( transaction, invocation.getArgument( 0 ) ) );
    }

    @Test
    void shouldNotTouchValuesThatDoNotNeedConversion()
    {
        // Given
        ListValue nodeList = VirtualValues.list( nodeEntityValue, directNodeValue );
        ListValue relationshipList = VirtualValues.list( relationshipEntityValue, directRelationshipValue );
        MapValue nodeMap = VirtualValues.map( new String[]{"a", "b"}, new AnyValue[]{nodeEntityValue, directNodeValue} );
        MapValue relationshipMap = VirtualValues.map( new String[]{"a", "b"}, new AnyValue[]{relationshipEntityValue, directRelationshipValue} );

        // Verify
        verifyDoesNotTouchValue( nodeEntityValue );
        verifyDoesNotTouchValue( relationshipEntityValue );
        verifyDoesNotTouchValue( directNodeValue );
        verifyDoesNotTouchValue( directRelationshipValue );
        verifyDoesNotTouchValue( nodeList );
        verifyDoesNotTouchValue( relationshipList );
        verifyDoesNotTouchValue( nodeMap );
        verifyDoesNotTouchValue( relationshipMap );

        // This is not an exhaustive test since the other cases are very uninteresting...
        verifyDoesNotTouchValue( Values.booleanValue( false ) );
        verifyDoesNotTouchValue( Values.stringValue( "Hello" ) );
        verifyDoesNotTouchValue( Values.longValue( 42L ) );
    }

    @Test
    void shouldConvertValuesWithVirtualEntities()
    {
        // Given
        ListValue nodeList = VirtualValues.list( nodeEntityValue, directNodeValue, nodeReference );
        ListValue expectedNodeList = VirtualValues.list( nodeEntityValue, directNodeValue, nodeEntityValue );

        ListValue relationshipList = VirtualValues.list( relationshipEntityValue, directRelationshipValue, relationshipReference );
        ListValue expectedRelationshipList = VirtualValues.list( relationshipEntityValue, directRelationshipValue, relationshipEntityValue );

        MapValue nodeMap = VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{nodeEntityValue, directNodeValue, nodeReference} );
        MapValue expectedNodeMap = VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{nodeEntityValue, directNodeValue, nodeEntityValue} );

        MapValue relationshipMap =
                VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{relationshipEntityValue, directRelationshipValue, relationshipReference} );
        MapValue expectedRelationshipMap =
                VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{relationshipEntityValue, directRelationshipValue, relationshipEntityValue} );

        ListValue nestedNodeList = VirtualValues.list( nodeList, nodeMap, nodeReference );
        ListValue expectedNestedNodeList = VirtualValues.list( expectedNodeList, expectedNodeMap, nodeEntityValue );

        ListValue nestedRelationshipList = VirtualValues.list( relationshipList, relationshipMap, relationshipReference );
        ListValue expectedNestedRelationshipList = VirtualValues.list( expectedRelationshipList, expectedRelationshipMap, relationshipEntityValue );

        MapValue nestedNodeMap = VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{nodeList, nodeMap, nestedNodeList} );
        MapValue expectedNestedNodeMap =
                VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{expectedNodeList, expectedNodeMap, expectedNestedNodeList} );

        MapValue nestedRelationshipMap =
                VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{relationshipList, relationshipMap, nestedRelationshipList} );
        MapValue expectedNestedRelationshipMap = VirtualValues.map( new String[]{"a", "b", "c"},
                new AnyValue[]{expectedRelationshipList, expectedRelationshipMap, expectedNestedRelationshipList} );

        // Verify
        verifyConvertsValue( expectedNodeList, nodeList );
        verifyConvertsValue( expectedRelationshipList, relationshipList );

        verifyConvertsValue( expectedNodeMap, nodeMap );
        verifyConvertsValue( expectedRelationshipMap, relationshipMap );

        verifyConvertsValue( expectedNestedNodeList, nestedNodeList );
        verifyConvertsValue( expectedNestedRelationshipList, nestedRelationshipList );

        verifyConvertsValue( expectedNestedNodeMap, nestedNodeMap );
        verifyConvertsValue( expectedNestedRelationshipMap, nestedRelationshipMap );
    }

    private void verifyConvertsValue( AnyValue expected, AnyValue valueToTest )
    {
        AnyValue actual = CompiledMaterializeValueMapper.mapAnyValue( transaction, valueToTest );
        assertEquals( expected, actual );
    }

    private void verifyDoesNotTouchValue( AnyValue value )
    {
        AnyValue mappedValue = CompiledMaterializeValueMapper.mapAnyValue( transaction, value );
        assertSame( value, mappedValue ); // Test with reference equality since we should get the same reference back
    }
}
