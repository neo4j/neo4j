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
package org.neo4j.com.storecopy;

import org.junit.Rule;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.TransactionObligationResponse;
import org.neo4j.function.Suppliers;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.com.StoreIdTestFactory.newStoreIdForCurrentVersion;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

public class ResponsePackerIT
{
    @Rule
    public final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void shouldPackTheHighestTxCommittedAsObligation() throws Exception
    {
        // GIVEN
        LogicalTransactionStore transactionStore = mock( LogicalTransactionStore.class );
        FileSystemAbstraction fs = fsRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );

        try ( NeoStores neoStore = createNeoStore( fs, pageCache ) )
        {
            MetaDataStore store = neoStore.getMetaDataStore();
            store.transactionCommitted( 2, 111, BASE_TX_COMMIT_TIMESTAMP );
            store.transactionCommitted( 3, 222, BASE_TX_COMMIT_TIMESTAMP );
            store.transactionCommitted( 4, 333, BASE_TX_COMMIT_TIMESTAMP );
            store.transactionCommitted( 5, 444, BASE_TX_COMMIT_TIMESTAMP );
            store.transactionCommitted( 6, 555, BASE_TX_COMMIT_TIMESTAMP );

            // skip 7 to emulate the fact we have an hole in the committed tx ids list

            final long expectedTxId = 8L;
            store.transactionCommitted( expectedTxId, 777, BASE_TX_COMMIT_TIMESTAMP );

            ResponsePacker packer =
                    new ResponsePacker( transactionStore, store, Suppliers.singleton( newStoreIdForCurrentVersion() ) );

            // WHEN
            Response<Object> response =
                    packer.packTransactionObligationResponse( new RequestContext( 0, 0, 0, 0, 0 ), new Object() );

            // THEN
            assertTrue( response instanceof TransactionObligationResponse );
            ((TransactionObligationResponse) response).accept( new Response.Handler()
            {
                @Override
                public void obligation( long txId )
                {
                    assertEquals( expectedTxId, txId );
                }

                @Override
                public Visitor<CommittedTransactionRepresentation,Exception> transactions()
                {
                    throw new UnsupportedOperationException( "not expected" );
                }
            } );
        }
    }

    private NeoStores createNeoStore( FileSystemAbstraction fs, PageCache pageCache ) throws IOException
    {
        File storeDir = new File( "/store/" );
        fs.mkdirs( storeDir );
        Config config = Config.defaults();
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory( fs );
        NullLogProvider logProvider = NullLogProvider.getInstance();
        StoreFactory storeFactory =
                new StoreFactory( storeDir, config, idGeneratorFactory, pageCache, fs, logProvider, EmptyVersionContextSupplier.EMPTY );
        return storeFactory.openAllNeoStores( true );
    }
}
