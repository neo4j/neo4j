/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommandApplierFacade;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockGroup;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.Abstract64BitRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.command.Command;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.NodeCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoCommandHandler;
import org.neo4j.kernel.impl.nioneo.xa.command.NeoTransactionStoreApplier;
import org.neo4j.kernel.impl.transaction.xaframework.PhysicalTransactionRepresentation;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.PageCacheRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import static org.neo4j.kernel.impl.api.TransactionRepresentationStoreApplier.DEFAULT_HIGH_ID_TRACKING;
import static org.neo4j.kernel.impl.nioneo.store.StoreFactory.configForStoreDir;
import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class ApplyRecoveredTransactionsTest
{
    @Test
    public void shouldSetCorrectHighIdWhenApplyingRecoveredTransactions() throws Exception
    {
        // WHEN recovering a transaction that creates some data
        long nodeId = neoStore.getNodeStore().nextId();
        long relationshipId = neoStore.getRelationshipStore().nextId();
        int type = 1;
        applyRecoveredTransaction( 1,
                nodeCommand( node( nodeId ), inUse( created( node( nodeId ) ) ) ),
                relationshipCommand( inUse( created( with( relationship( relationshipId ), nodeId, nodeId, type ) ) ) ) );
        
        // and when, later on, recovering a transaction deleting some of those
        applyRecoveredTransaction( 2,
                nodeCommand( inUse( created( node( nodeId ) ) ), node( nodeId ) ),
                relationshipCommand( relationship( relationshipId ) ) );
        
        // THEN that should be possible and the high ids should be correct, i.e. highest applied + 1
        assertEquals( nodeId+1, neoStore.getNodeStore().getHighId() );
        assertEquals( relationshipId+1, neoStore.getRelationshipStore().getHighId() );
    }

    private RelationshipRecord with( RelationshipRecord relationship, long startNode, long endNode, int type )
    {
        relationship.setFirstNode( startNode );
        relationship.setSecondNode( endNode );
        relationship.setType( type );
        return relationship;
    }

    private Command relationshipCommand( RelationshipRecord relationship )
    {
        RelationshipCommand command = new RelationshipCommand();
        command.init( relationship );
        return command;
    }

    private RelationshipRecord relationship( long relationshipId )
    {
        return new RelationshipRecord( relationshipId );
    }

    private void applyRecoveredTransaction( long transactionId, Command...commands ) throws IOException
    {
        NeoTransactionStoreApplier applier = new NeoTransactionStoreApplier( neoStore, mock( IndexingService.class ),
                mock( CacheAccessBackDoor.class ), mock( LockService.class ), new LockGroup(), transactionId,
                DEFAULT_HIGH_ID_TRACKING, true );
        CommandApplierFacade applierFacade = new CommandApplierFacade( applier,
                mock( NeoCommandHandler.class ), mock( NeoCommandHandler.class ), mock( NeoCommandHandler.class ) );
        new PhysicalTransactionRepresentation( Arrays.asList( commands ) ).accept( applierFacade );
    }
    
    public final @Rule EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    public final @Rule PageCacheRule pageCacheRule = new PageCacheRule();
    private NeoStore neoStore;
    
    @Before
    public void before()
    {
        File storeDir = new File( "dir" );
        Config config = configForStoreDir( new Config(), storeDir );
        StoreFactory storeFactory = new StoreFactory( config, new DefaultIdGeneratorFactory(),
                pageCacheRule.getPageCache( fsr.get(), config ), fsr.get(), DEV_NULL, new Monitors() );
        neoStore = storeFactory.newNeoStore( true );
    }

    @After
    public void after()
    {
        neoStore.close();
    }

    private Command nodeCommand( NodeRecord before, NodeRecord after )
    {
        NodeCommand command = new NodeCommand();
        command.init( before, after );
        return command;
    }

    private <RECORD extends Abstract64BitRecord> RECORD inUse( RECORD record )
    {
        record.setInUse( true );
        return record;
    }

    private <RECORD extends Abstract64BitRecord> RECORD created( RECORD record )
    {
        record.setCreated();
        return record;
    }

    private NodeRecord node( long id )
    {
        return new NodeRecord( id );
    }

    private long nextNodeId()
    {
        return neoStore.getNodeStore().nextId();
    }
}
