/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.nioneo.xa;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.store.IndexRule.State.POPULATING;
import static org.neo4j.kernel.impl.util.StringLogger.SYSTEM;

import java.io.File;
import java.util.Collection;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class WriteTransactionTest
{
    @Test
    public void shouldAddSchemaRuleToCacheWhenApplyingTransactionThatCreatesOne() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, null );
        writeTransaction.setCommitTxId( 1 );

        // WHEN
        final long ruleId = schemaStore.nextId();
        IndexRule schemaRule = new IndexRule( ruleId, 10, POPULATING, new long[] {8} );
        writeTransaction.createSchemaRule( schemaRule );
        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        verify( cacheAccessBackDoor ).addSchemaRule( schemaRule );
    }

    @Test
    public void shouldRemoveSchemaRuleFromCacheWhenApplyingTransactionThatDeletesOne() throws Exception
    {
        // GIVEN
        long labelId = 10, propertyKey = 10;
        IndexRule rule = new IndexRule( schemaStore.nextId(), labelId, POPULATING, new long[] {propertyKey} );
        Collection<DynamicRecord> records = schemaStore.allocateFrom( rule );
        for ( DynamicRecord record : records )
            schemaStore.updateRecord( record );
        long ruleId = first( records ).getId();
        WriteTransaction writeTransaction = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, null );
        writeTransaction.setCommitTxId( 1 );

        // WHEN
        writeTransaction.dropSchemaRule( ruleId );
        writeTransaction.prepare();
        writeTransaction.commit();

        // THEN
        verify( cacheAccessBackDoor ).removeSchemaRuleFromCache( ruleId );
    }
    
    @Test
    public void shouldRemoveSchemaRuleWhenRollingBackTransaction() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, null );
        writeTransaction.setCommitTxId( 1 );

        // WHEN
        final long ruleId = schemaStore.nextId();
        writeTransaction.createSchemaRule( new IndexRule( ruleId, 10, POPULATING, new long[] {7} ) );
        writeTransaction.prepare();
        writeTransaction.rollback();

        // THEN
        verifyNoMoreInteractions( cacheAccessBackDoor );
    }

    private EphemeralFileSystemAbstraction fileSystemAbstraction;
    private XaLogicalLog log;
    private TransactionState transactionState;
    private NeoStore neoStore;
    private CacheAccessBackDoor cacheAccessBackDoor;
    private SchemaStore schemaStore;
    
    @Before
    public void before() throws Exception
    {
        fileSystemAbstraction = new EphemeralFileSystemAbstraction();
        log = mock( XaLogicalLog.class );
        transactionState = TransactionState.NO_STATE;
        neoStore = mock( NeoStore.class );
        schemaStore = ephemeralSchemaStore();
        when( neoStore.getSchemaStore() ).thenReturn( schemaStore );
        cacheAccessBackDoor = mock( CacheAccessBackDoor.class );
    }
    
    @After
    public void after() throws Exception
    {
        fileSystemAbstraction.shutdown();
    }
    
    private SchemaStore ephemeralSchemaStore()
    {
        Config config = new Config( stringMap() );
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
        DefaultWindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
        StoreFactory storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory, fileSystemAbstraction, SYSTEM,
                new DefaultTxHook() );
        File file = new File( "schema-store" );
        storeFactory.createSchemaStore( file );
        return storeFactory.newSchemaStore( file );
    }

    private ArgumentMatcher<Collection<DynamicRecord>> firstRecordIdMatching( final int ruleId )
    {
        return new ArgumentMatcher<Collection<DynamicRecord>>()
        {
            @Override
            public boolean matches( Object argument )
            {
                Collection<DynamicRecord> records = (Collection<DynamicRecord>) argument; 
                return first( records ).getId() == ruleId;
            }
        };
    }
}
