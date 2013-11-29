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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;

import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.core.CacheAccessBackDoor;
import org.neo4j.kernel.impl.core.TransactionState;
import org.neo4j.kernel.impl.locking.LockService;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.NodeRecord;
import org.neo4j.kernel.impl.nioneo.store.NodeStore;
import org.neo4j.kernel.impl.nioneo.store.PropertyRecord;
import org.neo4j.kernel.impl.nioneo.store.PropertyStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.RelationshipStore;
import org.neo4j.kernel.impl.transaction.xaframework.XaLogicalLog;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class WriteTransactionCommandOrderingTest
{
    private final AtomicReference<List<String>> currentRecording = new AtomicReference<>();
    private final NeoStore store = mock( NeoStore.class );
    private final RecordingRelationshipStore relationshipStore = new RecordingRelationshipStore( currentRecording );
    private final RecordingNodeStore nodeStore = new RecordingNodeStore( currentRecording );
    private final RecordingPropertyStore propertyStore = new RecordingPropertyStore( currentRecording );

    public WriteTransactionCommandOrderingTest()
    {
        when( store.getPropertyStore() ).thenReturn( propertyStore );
        when( store.getNodeStore() ).thenReturn( nodeStore );
        when( store.getRelationshipStore() ).thenReturn( relationshipStore );
    }

    @Test
    public void shouldExecuteCommandsInTheSameOrderRegardlessOfItBeingRecoveredOrNot() throws Exception
    {
        // Given
        List<String> nonRecoveredRecording = new ArrayList<>();
        WriteTransaction nonRecoveredTx = newWriteTransaction();
        injectAllPossibleCommands( nonRecoveredTx );

        List<String> recoveredRecording = new ArrayList<>();
        WriteTransaction recoveredTx = newWriteTransaction();
        recoveredTx.setRecovered();
        injectAllPossibleCommands( recoveredTx );

        // When
        currentRecording.set( nonRecoveredRecording );
        nonRecoveredTx.doPrepare();
        nonRecoveredTx.doCommit();

        currentRecording.set( recoveredRecording );
        recoveredTx.doPrepare();
        recoveredTx.doCommit();

        // Then
        assertThat(nonRecoveredRecording, equalTo(recoveredRecording)); // ordering is the same in both cases
        assertThat(new HashSet<>( recoveredRecording ).size(), is( 9 )); // we have included all possible commands
    }

    private void injectAllPossibleCommands( WriteTransaction tx )
    {
        tx.injectCommand( new Command.NodeCommand( nodeStore, inUseNode(), inUseNode() ) ); // update
        tx.injectCommand( new Command.NodeCommand( nodeStore, inUseNode(), missingNode() ) ); // delete
        tx.injectCommand( new Command.NodeCommand( nodeStore, missingNode(), createdNode() ) ); // create
        tx.injectCommand( new Command.PropertyCommand( propertyStore, inUseProperty(), inUseProperty() ) ); // update
        tx.injectCommand( new Command.PropertyCommand( propertyStore, inUseProperty(), missingProperty() ) ); // delete
        tx.injectCommand( new Command.PropertyCommand( propertyStore, missingProperty(), createdProperty() ) ); // create
        tx.injectCommand( new Command.RelationshipCommand( relationshipStore, inUseRelationship() ) ); // update
        tx.injectCommand( new Command.RelationshipCommand( relationshipStore, missingRelationship() ) ); // delete
        tx.injectCommand( new Command.RelationshipCommand( relationshipStore, createdRelationship() ) ); // create
    }

    private static RelationshipRecord missingRelationship()
    {
        return new RelationshipRecord( -1 );
    }

    private static RelationshipRecord createdRelationship()
    {
        RelationshipRecord record = new RelationshipRecord( 2 );
        record.setInUse( true );
        record.setCreated();
        return record;
    }

    private static RelationshipRecord inUseRelationship()
    {
        RelationshipRecord record = new RelationshipRecord( 1 );
        record.setInUse( true );
        return record;
    }

    private static PropertyRecord missingProperty()
    {
        return new PropertyRecord( -1 );
    }

    private static PropertyRecord createdProperty()
    {
        PropertyRecord record = new PropertyRecord( 2 );
        record.setInUse( true );
        record.setCreated();
        return record;
    }

    private static PropertyRecord inUseProperty()
    {
        PropertyRecord record = new PropertyRecord( 1 );
        record.setInUse( true );
        return record;
    }

    private static NodeRecord missingNode()
    {
        return new NodeRecord(-1, -1, -1);
    }

    private static NodeRecord createdNode()
    {
        NodeRecord record = new NodeRecord( 2, -1, -1 );
        record.setInUse( true );
        record.setCreated();
        return record;
    }

    private static NodeRecord inUseNode()
    {
        NodeRecord record = new NodeRecord( 1, -1, -1 );
        record.setInUse( true );
        return record;
    }

    private WriteTransaction newWriteTransaction() {
        WriteTransaction tx = new WriteTransaction( 0, 0l, mock( XaLogicalLog.class ), TransactionState.NO_STATE,
                store, mock( CacheAccessBackDoor.class ), mock( IndexingService.class ),
                WriteTransactionTest.NO_LABEL_SCAN_STORE, mock( IntegrityValidator.class ),
                mock( KernelTransactionImplementation.class ), mock( LockService.class, RETURNS_MOCKS ) );
        tx.setCommitTxId( store.getLastCommittedTx() + 1 );
        return tx;
    }

    private static String commandActionToken( AbstractBaseRecord record )
    {
        if ( !record.inUse() )
        {
            return "deleted";
        }
        if ( record.isCreated() )
        {
            return "created";
        }
        return "updated";
    }

    private static class RecordingPropertyStore extends PropertyStore
    {
        private final AtomicReference<List<String>> currentRecording;

        public RecordingPropertyStore( AtomicReference<List<String>> currentRecording )
        {
            super( null, null, null, null, null, null, null, null, null );
            this.currentRecording = currentRecording;
        }

        @Override
        public void updateRecord(PropertyRecord record) {
            currentRecording.get().add(commandActionToken(record) + " property");
        }

        @Override
        protected void checkStorage() {
        }

        @Override
        protected void checkVersion() {
        }

        @Override
        protected void loadStorage() {
        }
    }

    private static class RecordingNodeStore extends NodeStore
    {
        private final AtomicReference<List<String>> currentRecording;

        public RecordingNodeStore( AtomicReference<List<String>> currentRecording )
        {
            super( null, null, null, null, null, null, null );
            this.currentRecording = currentRecording;
        }

        @Override
        public void updateRecord(NodeRecord record) {
            currentRecording.get().add(commandActionToken(record) + " node");
        }

        @Override
        protected void checkStorage() {
        }

        @Override
        protected void checkVersion() {
        }

        @Override
        protected void loadStorage() {
        }

        @Override
        public NodeRecord getRecord(long id) {
            NodeRecord record = new NodeRecord(id, -1, -1);
            record.setInUse(true);
            return record;
        }
    }

    private static class RecordingRelationshipStore extends RelationshipStore
    {
        private final AtomicReference<List<String>> currentRecording;

        public RecordingRelationshipStore( AtomicReference<List<String>> currentRecording )
        {
            super( null, null, null, null, null, null );
            this.currentRecording = currentRecording;
        }

        @Override
        public void updateRecord(RelationshipRecord record) {
            currentRecording.get().add(commandActionToken(record) + " relationship");
        }

        @Override
        protected void checkStorage() {
        }

        @Override
        protected void checkVersion() {
        }

        @Override
        protected void loadStorage() {
        }
    }
}
