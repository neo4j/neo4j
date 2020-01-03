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
package org.neo4j.test;

import org.neo4j.kernel.impl.core.TokenHolder;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.store.DynamicArrayStore;
import org.neo4j.kernel.impl.store.DynamicStringStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MockedNeoStores
{
    private MockedNeoStores()
    {
    }

    @SuppressWarnings( {"unchecked", "rawtypes"} )
    public static NeoStores basicMockedNeoStores()
    {
        NeoStores neoStores = mock( NeoStores.class );

        // NodeStore - DynamicLabelStore
        NodeStore nodeStore = mock( NodeStore.class );
        when( neoStores.getNodeStore() ).thenReturn( nodeStore );

        // NodeStore - DynamicLabelStore
        DynamicArrayStore dynamicLabelStore = mock( DynamicArrayStore.class );
        when( nodeStore.getDynamicLabelStore() ).thenReturn( dynamicLabelStore );

        // RelationshipStore
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( neoStores.getRelationshipStore() ).thenReturn( relationshipStore );

        // RelationshipGroupStore
        RelationshipGroupStore relationshipGroupStore = mock( RelationshipGroupStore.class );
        when( neoStores.getRelationshipGroupStore() ).thenReturn( relationshipGroupStore );

        // PropertyStore
        PropertyStore propertyStore = mock( PropertyStore.class );
        when( neoStores.getPropertyStore() ).thenReturn( propertyStore );

        // PropertyStore -- DynamicStringStore
        DynamicStringStore propertyStringStore = mock( DynamicStringStore.class );
        when( propertyStore.getStringStore() ).thenReturn( propertyStringStore );

        // PropertyStore -- DynamicArrayStore
        DynamicArrayStore propertyArrayStore = mock( DynamicArrayStore.class );
        when( propertyStore.getArrayStore() ).thenReturn( propertyArrayStore );

        return neoStores;
    }

    public static TokenHolders mockedTokenHolders()
    {
        return new TokenHolders(
                mock( TokenHolder.class ),
                mock( TokenHolder.class ),
                mock( TokenHolder.class ) );
    }
}
