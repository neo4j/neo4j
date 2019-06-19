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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.lock.LockService;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.util.concurrent.WorkSync;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

@EphemeralPageCacheExtension
class ApplyRecoveredTransactionsTest
{
    @Inject
    private PageCache pageCache;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private EphemeralFileSystemAbstraction fs;

    private NeoStores neoStores;
    private DefaultIdGeneratorFactory idGeneratorFactory;

    @BeforeEach
    void before()
    {
        idGeneratorFactory = new DefaultIdGeneratorFactory( fs, immediate() );
        StoreFactory storeFactory =
                new StoreFactory( testDirectory.databaseLayout(), Config.defaults(), idGeneratorFactory, pageCache, fs, NullLogProvider.getInstance() );
        neoStores = storeFactory.openAllNeoStores( true );
    }

    @AfterEach
    void after()
    {
        neoStores.close();
    }

    @Test
    void shouldSetCorrectHighIdWhenApplyingExternalTransactions() throws Exception
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
        when( lockService.acquireNodeLock( anyLong(), any( LockService.LockType.class ) ) ).thenReturn( LockService.NO_LOCK );
        when( lockService.acquireRelationshipLock( anyLong(), any( LockService.LockType.class ) ) ).thenReturn( LockService.NO_LOCK );
        Map<IdType,WorkSync<IdGenerator,IdGeneratorUpdateWork>> idGeneratorWorkSyncs = new EnumMap<>( IdType.class );
        for ( IdType idType : IdType.values() )
        {
            idGeneratorWorkSyncs.put( idType, new WorkSync<>( idGeneratorFactory.get( idType ) ) );
        }

        NeoStoreBatchTransactionApplier applier = new NeoStoreBatchTransactionApplier( INTERNAL, neoStores, mock( CacheAccessBackDoor.class ),
                lockService, idGeneratorWorkSyncs );
        CommandsToApply tx = new GroupOfCommands( transactionId, commands );
        CommandHandlerContract.apply( applier, txApplier ->
        {
            tx.accept( txApplier );
            return false;
        }, new GroupOfCommands( transactionId, commands ) );
    }

    private static <RECORD extends AbstractBaseRecord> RECORD inUse( RECORD record )
    {
        record.setInUse( true );
        return record;
    }

    private static <RECORD extends AbstractBaseRecord> RECORD created( RECORD record )
    {
        record.setCreated();
        return record;
    }
}
