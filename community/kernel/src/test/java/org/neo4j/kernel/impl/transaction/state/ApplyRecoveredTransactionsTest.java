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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.id.DefaultIdGeneratorFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandlerContract;
import org.neo4j.kernel.impl.transaction.command.NeoStoreBatchTransactionApplier;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ApplyRecoveredTransactionsTest
{
    @Test
    public void shouldSetCorrectHighIdWhenApplyingExternalTransactions() throws Exception
    {
        // WHEN recovering a transaction that creates some data
        long nodeId = neoStores.getNodeStore().nextId();
        long relationshipId = neoStores.getRelationshipStore().nextId();
        int type = 1;
        applyExternalTransaction( 1,
                new NodeCommand( new NodeRecord( nodeId ), inUse( created( new NodeRecord( nodeId ) ) ) ),
                new RelationshipCommand( null,
                        inUse( created( with( new RelationshipRecord( relationshipId ), nodeId, nodeId, type ) ) ) ) );

        // and when, later on, recovering a transaction deleting some of those
        applyExternalTransaction( 2,
                new NodeCommand( inUse( created( new NodeRecord( nodeId ) ) ), new NodeRecord( nodeId ) ),
                new RelationshipCommand( null, new RelationshipRecord( relationshipId ) ) );

        // THEN that should be possible and the high ids should be correct, i.e. highest applied + 1
        assertEquals( nodeId + 1, neoStores.getNodeStore().getHighId() );
        assertEquals( relationshipId + 1, neoStores.getRelationshipStore().getHighId() );
    }

    private RelationshipRecord with( RelationshipRecord relationship, long startNode, long endNode, int type )
    {
        relationship.setFirstNode( startNode );
        relationship.setSecondNode( endNode );
        relationship.setType( type );
        return relationship;
    }

    private void applyExternalTransaction( long transactionId, Command...commands ) throws Exception
    {
        LockService lockService = mock( LockService.class );
        when( lockService.acquireNodeLock( anyLong(), any(LockService.LockType.class) )).thenReturn( LockService.NO_LOCK );
        when( lockService.acquireRelationshipLock( anyLong(), any(LockService.LockType.class) )).thenReturn( LockService.NO_LOCK );
        NeoStoreBatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( neoStores,
                mock( CacheAccessBackDoor.class ), lockService );
        TransactionRepresentation tx = new PhysicalTransactionRepresentation( Arrays.asList( commands ) );
        CommandHandlerContract.apply( applier, txApplier ->
        {
            tx.accept( txApplier );
            return false;
        }, new TransactionToApply( tx, transactionId ) );
    }

    @Rule
    public final EphemeralFileSystemRule fsr = new EphemeralFileSystemRule();
    @Rule
    public final PageCacheRule pageCacheRule = new PageCacheRule();
    private NeoStores neoStores;

    @Before
    public void before()
    {
        FileSystemAbstraction fs = fsr.get();
        File storeDir = new File( "dir" );
        StoreFactory storeFactory = new StoreFactory( storeDir, Config.defaults(), new DefaultIdGeneratorFactory( fs ),
                pageCacheRule.getPageCache( fs ), fs, NullLogProvider.getInstance(), EmptyVersionContextSupplier.EMPTY );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @After
    public void after()
    {
        neoStores.close();
    }

    private <RECORD extends AbstractBaseRecord> RECORD inUse( RECORD record )
    {
        record.setInUse( true );
        return record;
    }

    private <RECORD extends AbstractBaseRecord> RECORD created( RECORD record )
    {
        record.setCreated();
        return record;
    }
}
