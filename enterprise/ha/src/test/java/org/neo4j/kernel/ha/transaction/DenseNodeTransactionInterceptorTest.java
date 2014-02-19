/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.ha.transaction;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.IdSequence;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipGroupRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.nioneo.xa.LogEntryVerifyingOutput;
import org.neo4j.kernel.impl.nioneo.xa.RecordAccessSet;
import org.neo4j.kernel.impl.nioneo.xa.TransactionDataBuilder;
import org.neo4j.kernel.impl.nioneo.xa.TransactionWriter;
import org.neo4j.kernel.impl.transaction.xaframework.LogEntry;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.unsafe.batchinsert.DirectRecordAccessSet;

import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.util.StringLogger.DEV_NULL;

public class DenseNodeTransactionInterceptorTest
{
    @Test
    public void shouldConvertFirstRelationshipForNodeCreation() throws Exception
    {
        // GIVEN the following store contents
        final Id nodeId = id(), relationshipId = id(), type = id();
        RecordAccessSet existingStore = existingStore( new ExistingContents()
        {
            @Override
            public void fill( NeoStore neoStore, TransactionDataBuilder transaction )
            {
                type.get( neoStore.getRelationshipTypeStore() );
                relationshipId.get( neoStore.getRelationshipStore() );
                transaction.create( node( nodeId.get( neoStore.getNodeStore() ) ) );
            }
        } );

        // WHEN this transaction is applied
        List<LogEntry> transaction = transaction( new TransactionContents()
        {
            @Override
            public void fill( TransactionDataBuilder transaction )
            {
                transaction.create( node( nodeId.get() )
                        .asInUse()
                        .withNextRel( relationshipId.get() ) );
                transaction.create( relationship( relationshipId.get(),
                        nodeId.get(), nodeId.get(), (int) type.get() )
                        .asInUse() );
            }
        } );
        List<LogEntry> translated = translate( existingStore, transaction );

        // THEN there should have been no change
        assertEquals( translated, transaction );
    }

    @Test
    public void shouldCreateTheFirstRelationshipOfNewTypeForDenseNode() throws Exception
    {
        // GIVEN the following store contents
        final Id denseNodeId = id(), otherNodeId = id(), thirdNodeId = id();
        final Id firstRelationshipId = id(), otherRelationshipId = id();
        final Id groupAId = id();
        final Id typeA = id(), typeB = id();
        RecordAccessSet existingStore = existingStore( new ExistingContents()
        {
            @Override
            public void fill( NeoStore neoStore, TransactionDataBuilder transaction )
            {
                transaction.create( group( groupAId.get( neoStore.getRelationshipGroupStore() ),
                        (int) typeA.get( neoStore.getRelationshipTypeStore() ) )
                        .asInUse()
                        .withFirstOut( firstRelationshipId.get( neoStore.getRelationshipStore() ) ) );
                transaction.create( node( denseNodeId.get( neoStore.getNodeStore() ) )
                        .asInUse()
                        .asDense()
                        .withNextRel( groupAId.get() ) );
                otherRelationshipId.get( neoStore.getRelationshipStore() );
                typeB.get( neoStore.getRelationshipGroupStore() );
                otherNodeId.get( neoStore.getNodeStore() );
                thirdNodeId.get( neoStore.getNodeStore() );
            }
        } );

        // WHEN this transaction that creates a relationship (otherRelationship) is applied
        List<LogEntry> translated = translate( existingStore, transaction( new TransactionContents()
        {
            @Override
            public void fill( TransactionDataBuilder transaction )
            {
                // This dense node is actually not dense as it looks when arriving, however on the receiving end
                // we have that node as dense.
                transaction.create( node( denseNodeId.get() )
                        .asInUse()
                        .withNextRel( otherRelationshipId.get() ) );
                transaction.create( relationship( otherRelationshipId.get(), thirdNodeId.get(), denseNodeId.get(),
                        (int) typeB.get() )
                        .asInUse()
                        .withEndPointers( -1, firstRelationshipId.get() ) );
                transaction.create( relationship( firstRelationshipId.get(), denseNodeId.get(), otherNodeId.get(),
                        (int) typeA.get() )
                        .asInUse()
                        .withStartPointers( otherRelationshipId.get(), -1 ) );
            }
        } ) );

        // THEN the translated version should have its representation translated to fit the local dense node
        assertTranslatedTransaction( translated, new TransactionContents()
        {
            @Override
            public void fill( TransactionDataBuilder transaction )
            {
                // the node doesn't need to be in this transaction, since it isn't changed in any other way,
                // but I think we'll include it always anyways for simplicity.
                transaction.create( node( denseNodeId.get() )
                        .asInUse()
                        .asDense()
                        .withNextRel( groupAId.get() ) );
                transaction.create( group( groupAId.get(), (int) typeA.get() )
                        .asInUse()
                        .withNextGroup( groupAId.get() +1/*bad assumption perhaps*/ )
                        .withFirstOut( firstRelationshipId.get() ) );
                transaction.create( group( groupAId.get()+1, (int) typeB.get() )
                        .asInUse()
                        .withFirstIn( otherRelationshipId.get() ) );
            }
        } );
    }

    // TODO should handle relationship creation where the node is dense and there are relationships of that type already
    // TODO should handle that both start and end nodes are dense
    // TODO should handle that a relationship gets created or deleted, and a surrounding
    //       relationship that will have its pointers updated, yet at the same time modified for
    //       some other reason (added/remove property) should still have the property change coming through.
    // TODO should handle bigger transaction with multiple relationships created in it

    /* TODO For deletions. We need that because:
     *
     * Master (2.0):
     *   Node --> Rel1_A --> Rel2_B --> Rel3_A
     *
     * Slave (2.1):
     *   Node --> GroupA --> GroupB
     *              |          |
     *              |          |
     *              v          v
     *           Rel1_A      Rel2_B
     *              |
     *              |
     *              v
     *           Rel3_A
     */

    public final @Rule CleanupRule cleanup = new CleanupRule();
    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    private List<LogEntry> transaction( TransactionContents contents ) throws IOException
    {
        List<LogEntry> entries = new ArrayList<>();
        TransactionWriter writer = new TransactionWriter( new TransactionWriter.CommandCollector( entries ), 1, 1 );
        TransactionDataBuilder builder = new TransactionDataBuilder( writer );

        writer.start( -1, 1, 1 );
        contents.fill( builder );
        writer.prepare();

        // The log entries have ended up in this list, so just return it
        return entries;
    }

    private List<LogEntry> translate( RecordAccessSet existingStore, List<LogEntry> transaction )
    {
        return new DenseNodeTransactionInterceptor( existingStore ).apply( transaction );
    }

    private interface ExistingContents
    {
        void fill( NeoStore neoStore, TransactionDataBuilder transaction );
    }

    private interface TransactionContents
    {
        void fill( TransactionDataBuilder transaction );
    }

    private RecordAccessSet existingStore( ExistingContents contents )
    {
        @SuppressWarnings( "deprecation" )
        StoreFactory storeFactory = new StoreFactory( new Config(), new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), fs.get(), DEV_NULL, new DefaultTxHook() );
        File storeFile = new File( "neostore" );
        NeoStore neoStore = cleanup.add( storeFactory.createNeoStore( storeFile ) );

        // A little piggy-backing on the TransactionWriter/TransactionDataBuilder abstraction
        TransactionWriter writer = new TransactionWriter( new TransactionWriter.RecordOutput(
                new TransactionWriter.NeoStoreCommandRecordVisitor( neoStore ) ), 0, 0 );
        contents.fill( neoStore, new TransactionDataBuilder( writer ) );
        neoStore.updateIdGenerators();

        return new DirectRecordAccessSet( neoStore );
    }

    private void assertTranslatedTransaction( List<LogEntry> translated, TransactionContents transactionContents )
    {
        LogEntryVerifyingOutput verifier = new LogEntryVerifyingOutput( translated );
        TransactionWriter writer = new TransactionWriter( verifier, 0, 0 );
        transactionContents.fill( new TransactionDataBuilder( writer ) ); // <-- this will assert its contents
        verifier.done();
    }

    private static class Id
    {
        private long id;
        private boolean captured;

        public long get( IdSequence sequence )
        {
            if ( captured )
            {
                throw new IllegalStateException( "Already captured" );
            }

            id = sequence.nextId();
            captured = true;
            return id;
        }

        public long get()
        {
            if ( !captured )
            {
                throw new IllegalStateException( "Not captured" );
            }
            return id;
        }
    }

    private static Id id()
    {
        return new Id();
    }

    private static class NodeRecordWithBenefits extends NodeRecord
    {
        public NodeRecordWithBenefits( long id, boolean dense, long nextRel, long nextProp )
        {
            super( id, dense, nextRel, nextProp );
        }

        public NodeRecordWithBenefits asInUse()
        {
            setInUse( true );
            return this;
        }

        public NodeRecordWithBenefits asDense()
        {
            setDense( true );
            return this;
        }

        public NodeRecordWithBenefits withNextRel( long id )
        {
            setNextRel( id );
            return this;
        }
    }

    private static NodeRecordWithBenefits node( long id )
    {
        return new NodeRecordWithBenefits( id, false, -1, -1 );
    }

    private static class RelationshipRecordWithBenefits extends RelationshipRecord
    {
        public RelationshipRecordWithBenefits( long id, long firstNode, long secondNode, int type )
        {
            super( id, firstNode, secondNode, type );
        }

        public RelationshipRecordWithBenefits withEndPointers( long prev, long next )
        {
            setSecondPrevRel( prev );
            setSecondNextRel( next );
            return this;
        }

        public RelationshipRecordWithBenefits withStartPointers( long prev, long next )
        {
            setFirstPrevRel( prev );
            setFirstNextRel( next );
            return this;
        }

        public RelationshipRecordWithBenefits asInUse()
        {
            setInUse( true );
            return this;
        }
    }

    private static RelationshipRecordWithBenefits relationship( long id, long startNode, long endNode, int type )
    {
        return new RelationshipRecordWithBenefits( id, startNode, endNode, type );
    }

    private static class RelationshipGroupRecordWithBenefits extends RelationshipGroupRecord
    {
        public RelationshipGroupRecordWithBenefits( long id, int type )
        {
            super( id, type );
        }

        public RelationshipGroupRecordWithBenefits asInUse()
        {
            setInUse( true );
            return this;
        }

        public RelationshipGroupRecordWithBenefits withNextGroup( long id )
        {
            setNext( id );
            return this;
        }

        public RelationshipGroupRecordWithBenefits withFirstOut( long id )
        {
            setFirstOut( id );
            return this;
        }

        public RelationshipGroupRecordWithBenefits withFirstIn( long id )
        {
            setFirstIn( id );
            return this;
        }

        public RelationshipGroupRecordWithBenefits withFirstLoop( long id )
        {
            setFirstLoop( id );
            return this;
        }
    }

    private static RelationshipGroupRecordWithBenefits group( long id, int type )
    {
        return new RelationshipGroupRecordWithBenefits( id, type );
    }
}
