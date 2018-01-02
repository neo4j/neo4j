/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.function.Supplier;
import org.neo4j.kernel.ha.transaction.OnDiskLastTxIdGetter;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.state.NeoStoresSupplier;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnDiskLastTxIdGetterTest
{
    @Rule
    public PageCacheRule pageCacheRule = new PageCacheRule();
    @Rule
    public EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void testGetLastTxIdNoFilePresent() throws Exception
    {
        // This is a sign that we have some bad coupling on our hands.
        // We currently have to do this because of our lifecycle and construction ordering.
        NeoStoresSupplier supplier = mock( NeoStoresSupplier.class );
        NeoStores neoStores = mock( NeoStores.class );
        MetaDataStore metaDataStore = mock( MetaDataStore.class );
        when( supplier.get() ).thenReturn( neoStores );
        when( neoStores.getMetaDataStore() ).thenReturn( metaDataStore );
        when( metaDataStore.getLastCommittedTransactionId() ).thenReturn( 13L );

        OnDiskLastTxIdGetter getter = new OnDiskLastTxIdGetter( supplier );
        assertEquals( 13L, getter.getLastTxId() );
    }

    @Test
    public void lastTransactionIdIsBaseTxIdWhileNeoStoresAreStopped()
    {
        final StoreFactory storeFactory = new StoreFactory( fs.get(), new File( "store" ),
                pageCacheRule.getPageCache( fs.get() ), NullLogProvider.getInstance() );
        final NeoStores neoStores = storeFactory.openAllNeoStores( true );
        neoStores.close();

        Supplier<NeoStores> neoStoresSupplier = new NeoStoresSupplier()
        {
            @Override
            public NeoStores get()
            {
                return neoStores;
            }
        };
        OnDiskLastTxIdGetter diskLastTxIdGetter = new OnDiskLastTxIdGetter( neoStoresSupplier );
        assertEquals( TransactionIdStore.BASE_TX_ID, diskLastTxIdGetter.getLastTxId() );
    }


}
