/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.log.Commitment;
import org.neo4j.kernel.impl.transaction.log.FakeCommitment;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.test.Race;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;
import static org.neo4j.storageengine.api.TransactionApplicationMode.INTERNAL;

public class LegacyBatchIndexApplierTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldOnlyCreateOneApplierPerProvider() throws Exception
    {
        // GIVEN
        Map<String,Integer> names = MapUtil.genericMap( "first", 0, "second", 1 );
        Map<String,Integer> keys = MapUtil.genericMap( "key", 0 );
        String applierName = "test-applier";
        Commitment commitment = mock( Commitment.class );
        when( commitment.hasLegacyIndexChanges() ).thenReturn( true );
        IndexConfigStore config = newIndexConfigStore( names, applierName );
        LegacyIndexApplierLookup applierLookup = mock( LegacyIndexApplierLookup.class );
        when( applierLookup.newApplier( anyString(), anyBoolean() ) ).thenReturn( mock( TransactionApplier.class ) );
        try ( LegacyBatchIndexApplier applier = new LegacyBatchIndexApplier( config, applierLookup, BYPASS, INTERNAL ) )
        {
            TransactionToApply tx = new TransactionToApply( null, 2 );
            tx.commitment( commitment, 2 );
            try ( TransactionApplier txApplier = applier.startTx( tx ) )
            {
                // WHEN
                IndexDefineCommand definitions = definitions( names, keys );
                txApplier.visitIndexDefineCommand( definitions );
                txApplier.visitIndexAddNodeCommand( addNodeToIndex( definitions, "first" ) );
                txApplier.visitIndexAddNodeCommand( addNodeToIndex( definitions, "second" ) );
                txApplier.visitIndexAddRelationshipCommand( addRelationshipToIndex( definitions, "second" ) );
            }
        }

        // THEN
        verify( applierLookup, times( 1 ) ).newApplier( eq( applierName ), anyBoolean() );
    }

    @Test
    public void shouldOrderTransactionsMakingLegacyIndexChanges() throws Throwable
    {
        // GIVEN
        Map<String,Integer> names = MapUtil.genericMap( "first", 0, "second", 1 );
        Map<String,Integer> keys = MapUtil.genericMap( "key", 0 );
        String applierName = "test-applier";
        LegacyIndexApplierLookup applierLookup = mock( LegacyIndexApplierLookup.class );
        when( applierLookup.newApplier( anyString(), anyBoolean() ) ).thenReturn( mock( TransactionApplier.class ) );
        IndexConfigStore config = newIndexConfigStore( names, applierName );

        // WHEN multiple legacy index transactions are running, they should be done in order
        SynchronizedArrayIdOrderingQueue queue = new SynchronizedArrayIdOrderingQueue( 10 );
        final AtomicLong lastAppliedTxId = new AtomicLong( -1 );
        Race race = new Race();
        for ( long i = 0; i < 100; i++ )
        {
            final long txId = i;
            race.addContestant( () -> {
                try ( LegacyBatchIndexApplier applier = new LegacyBatchIndexApplier( config, applierLookup, queue, INTERNAL ) )
                {
                    TransactionToApply txToApply = new TransactionToApply(
                            new PhysicalTransactionRepresentation( new ArrayList<>() ) );
                    FakeCommitment commitment = new FakeCommitment( txId, mock( TransactionIdStore.class ) );
                    commitment.setHasLegacyIndexChanges( true );
                    txToApply.commitment( commitment, txId );
                    TransactionApplier txApplier = applier.startTx( txToApply );

                    // Make sure threads are unordered
                    Thread.sleep( ThreadLocalRandom.current().nextInt( 5 ) );

                    // THEN
                    assertTrue( lastAppliedTxId.compareAndSet( txId - 1, txId ) );

                    // Closing manually instead of using try-with-resources since we have no additional work to do in
                    // txApplier
                    txApplier.close();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            } );
            queue.offer( txId );
        }

        race.go();
    }

    private static AddRelationshipCommand addRelationshipToIndex( IndexDefineCommand definitions, String indexName )
    {
        AddRelationshipCommand command = new AddRelationshipCommand();
        command.init( definitions.getOrAssignIndexNameId( indexName ), 0L, (byte) 0, null, 1, 2 );
        return command;
    }

    private static AddNodeCommand addNodeToIndex( IndexDefineCommand definitions, String indexName )
    {
        AddNodeCommand command = new AddNodeCommand();
        command.init( definitions.getOrAssignIndexNameId( indexName ), 0L, (byte) 0, null );
        return command;
    }

    private static IndexDefineCommand definitions( Map<String,Integer> names, Map<String,Integer> keys )
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init( names, keys );
        return definitions;
    }

    private IndexConfigStore newIndexConfigStore( Map<String,Integer> names, String providerName )
    {
        File dir = new File( "conf" );
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        fileSystem.mkdirs( dir );
        IndexConfigStore store = life.add( new IndexConfigStore( dir, fileSystem ) );
        for ( Map.Entry<String,Integer> name : names.entrySet() )
        {
            store.set( Node.class, name.getKey(), stringMap( IndexManager.PROVIDER, providerName ) );
            store.set( Relationship.class, name.getKey(), stringMap( IndexManager.PROVIDER, providerName ) );
        }
        return store;
    }
}
