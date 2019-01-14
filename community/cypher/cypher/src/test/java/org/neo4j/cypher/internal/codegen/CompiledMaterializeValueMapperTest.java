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

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.impl.core.GraphPropertiesProxy;
import org.neo4j.kernel.impl.core.NodeProxy;
import org.neo4j.kernel.impl.core.RelationshipProxy;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CompiledMaterializeValueMapperTest
{
    private EmbeddedProxySPI spi = new EmbeddedProxySPI()
    {
        @Override
        public RelationshipProxy newRelationshipProxy( long id )
        {
            return new RelationshipProxy( this, id );
        }

        @Override
        public NodeProxy newNodeProxy( long nodeId )
        {
            return new NodeProxy( this, nodeId );
        }

        @Override
        public Statement statement()
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public KernelTransaction kernelTransaction()
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public GraphDatabaseService getGraphDatabase()
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public void assertInUnterminatedTransaction()
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public void failTransaction()
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public RelationshipProxy newRelationshipProxy( long id, long startNodeId, int typeId, long endNodeId )
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public GraphPropertiesProxy newGraphPropertiesProxy()
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public RelationshipType getRelationshipTypeById( int type )
        {
            throw new IllegalStateException( "Should not be used" );
        }

        @Override
        public int getRelationshipTypeIdByName( String typeName )
        {
            throw new IllegalStateException( "Should not be used" );
        }
    };

    NodeValue nodeProxyValue = ValueUtils.fromNodeProxy( new NodeProxy( spi, 1L ) );
    NodeValue directNodeValue = VirtualValues.nodeValue( 2L, Values.stringArray(), VirtualValues.emptyMap() );
    NodeReference nodeReference = VirtualValues.node( 1L ); // Should equal nodeProxyValue when converted

    RelationshipValue relationshipProxyValue = ValueUtils.fromRelationshipProxy( new RelationshipProxy( spi, 11L ) );
    RelationshipValue directRelationshipValue =
            VirtualValues.relationshipValue( 12L, nodeProxyValue, directNodeValue, Values.stringValue( "TYPE" ), VirtualValues.emptyMap() );
    RelationshipReference relationshipReference = VirtualValues.relationship( 11L ); // Should equal relationshipProxyValue when converted

    @Test
    public void shouldNotTouchValuesThatDoNotNeedConversion()
    {
        // Given
        ListValue nodeList = VirtualValues.list( nodeProxyValue, directNodeValue );
        ListValue relationshipList = VirtualValues.list( relationshipProxyValue, directRelationshipValue );
        MapValue nodeMap = VirtualValues.map( new String[]{"a", "b"}, new AnyValue[]{nodeProxyValue, directNodeValue} );
        MapValue relationshipMap = VirtualValues.map( new String[]{"a", "b"}, new AnyValue[]{relationshipProxyValue, directRelationshipValue} );

        // Verify
        verifyDoesNotTouchValue( nodeProxyValue );
        verifyDoesNotTouchValue( relationshipProxyValue );
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
        // ...
    }

    @Test
    public void shouldConvertValuesWithVirtualEntities()
    {
        // Given
        ListValue nodeList = VirtualValues.list( nodeProxyValue, directNodeValue, nodeReference );
        ListValue expectedNodeList = VirtualValues.list( nodeProxyValue, directNodeValue, nodeProxyValue );

        ListValue relationshipList = VirtualValues.list( relationshipProxyValue, directRelationshipValue, relationshipReference );
        ListValue expectedRelationshipList = VirtualValues.list( relationshipProxyValue, directRelationshipValue, relationshipProxyValue );

        MapValue nodeMap = VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{nodeProxyValue, directNodeValue, nodeReference} );
        MapValue expectedNodeMap = VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{nodeProxyValue, directNodeValue, nodeProxyValue} );

        MapValue relationshipMap =
                VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{relationshipProxyValue, directRelationshipValue, relationshipReference} );
        MapValue expectedRelationshipMap =
                VirtualValues.map( new String[]{"a", "b", "c"}, new AnyValue[]{relationshipProxyValue, directRelationshipValue, relationshipProxyValue} );

        ListValue nestedNodeList = VirtualValues.list( nodeList, nodeMap, nodeReference );
        ListValue expectedNestedNodeList = VirtualValues.list( expectedNodeList, expectedNodeMap, nodeProxyValue );

        ListValue nestedRelationshipList = VirtualValues.list( relationshipList, relationshipMap, relationshipReference );
        ListValue expectedNestedRelationshipList = VirtualValues.list( expectedRelationshipList, expectedRelationshipMap, relationshipProxyValue );

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
        AnyValue actual = CompiledMaterializeValueMapper.mapAnyValue( spi, valueToTest );
        assertEquals( expected, actual );
    }

    private void verifyDoesNotTouchValue( AnyValue value )
    {
        AnyValue mappedValue = CompiledMaterializeValueMapper.mapAnyValue( spi, value );
        assertTrue( value == mappedValue ); // Test with reference equality since we should get the same reference back
    }
}
