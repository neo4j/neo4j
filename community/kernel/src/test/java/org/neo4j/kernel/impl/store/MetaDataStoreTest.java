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
package org.neo4j.kernel.impl.store;

import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP;

public class MetaDataStoreTest
{
    @ClassRule
    public static final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void getCreationTimeShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getCreationTime();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getCurrentLogVersionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getCurrentLogVersion();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getGraphNextPropShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getGraphNextProp();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastClosedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastClosedTransactionId();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastClosedTransactionShouldFailWhenStoreIsClosed() throws Exception
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastClosedTransaction();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastCommittedTransactionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastCommittedTransaction();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastCommittedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLastCommittedTransactionId();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLatestConstraintIntroducingTxShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getLatestConstraintIntroducingTx();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getRandomNumberShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getRandomNumber();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getStoreVersionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getStoreVersion();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getUpgradeTimeShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getUpgradeTime();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getUpgradeTransactionShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.getUpgradeTransaction();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void nextCommittingTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.nextCommittingTransactionId();
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void setLastCommittedAndClosedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.setLastCommittedAndClosedTransactionId( 1, 1, BASE_TX_COMMIT_TIMESTAMP, 1, 1 );
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void transactionCommittedShouldFailWhenStoreIsClosed() throws IOException
    {
        MetaDataStore metaDataStore = newMetaDataStore();
        metaDataStore.close();
        try
        {
            metaDataStore.transactionCommitted( 1, 1, BASE_TX_COMMIT_TIMESTAMP );
            fail( "Expected exception reading from MetaDataStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void lastTxCommitTimestampShouldBeBaseInNewStore() throws Exception
    {
        try ( MetaDataStore metaDataStore = newMetaDataStore() )
        {
            long timestamp = metaDataStore.getLastCommittedTransaction().commitTimestamp();
            assertThat( timestamp, equalTo( TransactionIdStore.BASE_TX_COMMIT_TIMESTAMP ) );
        }
    }

    private static MetaDataStore newMetaDataStore() throws IOException
    {
        EphemeralFileSystemAbstraction fs = MetaDataStoreTest.fsRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        File dir = new File( "store" );
        StoreFactory storeFactory = new StoreFactory( fs, dir, pageCache, NullLogProvider.getInstance() );
        return storeFactory.openNeoStores( true, NeoStores.StoreType.META_DATA ).getMetaDataStore();
    }
}
