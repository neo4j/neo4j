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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.StringLogger.SYSTEM;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.PropertyIndex;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaStore;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.xaframework.DefaultLogBufferFactory;
import org.neo4j.kernel.impl.transaction.xaframework.LogPruneStrategies;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;
import org.neo4j.kernel.logging.SingleLoggingService;
import org.neo4j.test.impl.EphemeralFileSystemAbstraction;

public class WriteTransactionTest
{
    @Test
    public void shouldAddSchemaRuleToCacheWhenApplyingTransactionThatCreatesOne() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, NO_INDEXING );
        writeTransaction.setCommitTxId( 1 );

        // WHEN
        final long ruleId = schemaStore.nextId();
        IndexRule schemaRule = new IndexRule( ruleId, 10, 8 );
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
        IndexRule rule = new IndexRule( schemaStore.nextId(), labelId, propertyKey );
        Collection<DynamicRecord> records = schemaStore.allocateFrom( rule );
        for ( DynamicRecord record : records )
            schemaStore.updateRecord( record );
        long ruleId = first( records ).getId();
        WriteTransaction writeTransaction = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, NO_INDEXING );
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
                cacheAccessBackDoor, NO_INDEXING );
        writeTransaction.setCommitTxId( 1 );

        // WHEN
        final long ruleId = schemaStore.nextId();
        writeTransaction.createSchemaRule( new IndexRule( ruleId, 10, 7 ) );
        writeTransaction.prepare();
        writeTransaction.rollback();

        // THEN
        verifyNoMoreInteractions( cacheAccessBackDoor );
    }
    
    @Test
    public void shouldWriteProperBeforeAndAfterPropertyRecordsWhenAddingProperty() throws Exception
    {
        // GIVEN
        WriteTransaction writeTransaction = new WriteTransaction( 0, log, transactionState, neoStore,
                cacheAccessBackDoor, NO_INDEXING );
        int nodeId = 1;
        writeTransaction.setCommitTxId( nodeId );
        writeTransaction.nodeCreate( nodeId );
        PropertyIndex propertyIndex = new PropertyIndex( "key", 1 );
        Object value = 5;

        // WHEN
        writeTransaction.nodeAddProperty( nodeId, propertyIndex, value );
        writeTransaction.doPrepare();

        // THEN
        PropertyCommand propertyCommand = (PropertyCommand) single( filter( new Predicate<XaCommand>()
        {
            @Override
            public boolean accept( XaCommand item )
            {
                return item instanceof PropertyCommand;
            }
        }, log.commands ) );
        PropertyRecord before = propertyCommand.getBefore();
        assertFalse( before.inUse() );
        assertEquals( Collections.emptyList(), before.getPropertyBlocks() );

        PropertyRecord after = propertyCommand.getAfter();
        assertTrue( after.inUse() );
        assertEquals( 1, count( after.getPropertyBlocks() ) );
    }
    
    // TODO change property record
    // TODO remove property record

    private EphemeralFileSystemAbstraction fileSystemAbstraction;
    private CapturingXaLogicalLog log;
    private TransactionState transactionState;
    private final Config config = new Config( stringMap() );
    private final DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory();
    private final DefaultWindowPoolFactory windowPoolFactory = new DefaultWindowPoolFactory();
    private StoreFactory storeFactory;
    private NeoStore neoStore;
    private CacheAccessBackDoor cacheAccessBackDoor;
    private SchemaStore schemaStore;
    private PropertyStore propertyStore;
    
    @Before
    public void before() throws Exception
    {
        fileSystemAbstraction = new EphemeralFileSystemAbstraction();
        log = new CapturingXaLogicalLog( fileSystemAbstraction );
        transactionState = TransactionState.NO_STATE;
        neoStore = mock( NeoStore.class );
        storeFactory = new StoreFactory( config, idGeneratorFactory, windowPoolFactory,
                fileSystemAbstraction, SYSTEM, new DefaultTxHook() );
        
        File schemaStoreFile = new File( "schema-store" );
        storeFactory.createSchemaStore( schemaStoreFile );
        schemaStore = storeFactory.newSchemaStore( schemaStoreFile );
        when( neoStore.getSchemaStore() ).thenReturn( schemaStore );
        
        File propertyStoreFile = new File( "property-store" );
        storeFactory.createPropertyStore( propertyStoreFile );
        propertyStore = storeFactory.newPropertyStore( propertyStoreFile );
        when( neoStore.getPropertyStore() ).thenReturn( propertyStore );
        
        cacheAccessBackDoor = mock( CacheAccessBackDoor.class );
    }
    
    private static class CapturingXaLogicalLog extends XaLogicalLog
    {
        private final Collection<XaCommand> commands = new ArrayList<XaCommand>();

        public CapturingXaLogicalLog( FileSystemAbstraction fs )
        {
            super( new File( "log" ), null, null, null, new DefaultLogBufferFactory(),
                    fs, new SingleLoggingService( SYSTEM ), LogPruneStrategies.NO_PRUNING, null );
        }
        
        @Override
        public synchronized void writeCommand( XaCommand command, int identifier ) throws IOException
        {
            commands.add( command );
//            super.writeCommand( command, identifier );
        }
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

    static IndexingService NO_INDEXING = mock(IndexingService.class);
}
