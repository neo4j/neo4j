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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.SchemaStore;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecordChangeSetTest
{
    @Test
    public void shouldStartWithSetsInitializedAndEmpty() throws Exception
    {
        // GIVEN
        RecordChangeSet changeSet = new RecordChangeSet( mock( NeoStores.class ) );

        // WHEN
        // nothing really

        // THEN
        assertEquals( 0, changeSet.getNodeRecords().changeSize() );
        assertEquals( 0, changeSet.getPropertyRecords().changeSize() );
        assertEquals( 0, changeSet.getRelRecords().changeSize() );
        assertEquals( 0, changeSet.getSchemaRuleChanges().changeSize() );
        assertEquals( 0, changeSet.getRelGroupRecords().changeSize() );
    }

    @Test
    public void shouldClearStateOnClose() throws Exception
    {
        // GIVEN
        NeoStores mockStore = mock( NeoStores.class );
        when( mockStore.getNodeStore() ).thenReturn( mock( NodeStore.class ) );
        when( mockStore.getRelationshipStore() ).thenReturn( mock( RelationshipStore.class ) );
        when( mockStore.getPropertyStore() ).thenReturn( mock( PropertyStore.class ) );
        when( mockStore.getSchemaStore() ).thenReturn( mock( SchemaStore.class ) );
        when( mockStore.getRelationshipGroupStore() ).thenReturn( mock( RelationshipGroupStore.class ) );

        RecordChangeSet changeSet = new RecordChangeSet( mockStore );

        // WHEN
        /*
         * We need to make sure some stuff is stored in the sets being managed. That is why forChangingLinkage() is
         * called - otherwise, no changes will be stored and changeSize() would return 0 anyway.
         */
        changeSet.getNodeRecords().create( 1l, null ).forChangingLinkage();
        changeSet.getPropertyRecords().create( 1l, null ).forChangingLinkage();
        changeSet.getRelRecords().create( 1l, null ).forChangingLinkage();
        changeSet.getSchemaRuleChanges().create( 1l, null ).forChangingLinkage();
        changeSet.getRelGroupRecords().create( 1l, 1 ).forChangingLinkage();

        changeSet.close();

        // THEN
        assertEquals( 0, changeSet.getNodeRecords().changeSize() );
        assertEquals( 0, changeSet.getPropertyRecords().changeSize() );
        assertEquals( 0, changeSet.getRelRecords().changeSize() );
        assertEquals( 0, changeSet.getSchemaRuleChanges().changeSize() );
        assertEquals( 0, changeSet.getRelGroupRecords().changeSize() );
    }
}
