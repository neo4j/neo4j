/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class NeoStoreTest
{
    @ClassRule
    public static final EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();

    @ClassRule
    public static final PageCacheRule pageCacheRule = new PageCacheRule();

    @Test
    public void getCreationTimeShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getCreationTime();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getCurrentLogVersionShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getCurrentLogVersion();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getGraphNextPropShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getGraphNextProp();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastClosedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getLastClosedTransactionId();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastCommittedTransactionShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getLastCommittedTransaction();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLastCommittedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getLastCommittedTransactionId();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getLatestConstraintIntroducingTxShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getLatestConstraintIntroducingTx();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getRandomNumberShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getRandomNumber();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getStoreVersionShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getStoreVersion();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getUpgradeTimeShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getUpgradeTime();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void getUpgradeTransactionShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.getUpgradeTransaction();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void nextCommittingTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.nextCommittingTransactionId();
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void logIdUsageShouldNotThrowExceptionWithNullIdGenerator() throws IOException
    {
        // Given
        NeoStore neoStore = newNeoStore();
        neoStore.visitStore( new Visitor<CommonAbstractStore,RuntimeException>()
        {
            @Override
            public boolean visit( CommonAbstractStore store ) throws RuntimeException
            {
                // Make sure id generator is null
                store.deleteIdGenerator();
                return false;
            }
        } );

        // When
        neoStore.logIdUsage( mock( StringLogger.LineLogger.class ) );
        // Then should not throw exception
        // Cleanup
        neoStore.close();
    }

    @Test
    public void setLastCommittedAndClosedTransactionIdShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.setLastCommittedAndClosedTransactionId( 1, 1 );
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    @Test
    public void transactionCommittedShouldFailWhenStoreIsClosed() throws IOException
    {
        NeoStore neoStore = newNeoStore();
        neoStore.close();
        try
        {
            neoStore.transactionCommitted( 1, 1 );
            fail( "Expected exception reading from NeoStore after being closed." );
        }
        catch ( Exception e )
        {
            assertThat( e, instanceOf( IllegalStateException.class ) );
        }
    }

    private static NeoStore newNeoStore() throws IOException
    {
        EphemeralFileSystemAbstraction fs = NeoStoreTest.fsRule.get();
        PageCache pageCache = pageCacheRule.getPageCache( fs );
        File dir = new File( "store" );
        StoreFactory storeFactory = new StoreFactory( fs, dir, pageCache, StringLogger.DEV_NULL, new Monitors() );
        return storeFactory.newNeoStore( true );
    }
}
