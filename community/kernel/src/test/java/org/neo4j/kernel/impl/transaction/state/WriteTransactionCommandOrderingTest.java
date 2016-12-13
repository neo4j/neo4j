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
package org.neo4j.kernel.impl.transaction.state;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommandVisitor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.log.PhysicalTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.transaction.state.RecordChanges.RecordChange;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageCommand;
import org.neo4j.storageengine.api.schema.SchemaRule;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class WriteTransactionCommandOrderingTest
{
    private final AtomicReference<List<String>> currentRecording = new AtomicReference<>();
    private final RecordingRelationshipStore relationshipStore = new RecordingRelationshipStore( currentRecording );
    private final RecordingNodeStore nodeStore = new RecordingNodeStore( currentRecording );
    private final RecordingPropertyStore propertyStore = new RecordingPropertyStore( currentRecording );
    private final NeoStores store = mock( NeoStores.class );

    public WriteTransactionCommandOrderingTest()
    {
        when( store.getPropertyStore() ).thenReturn( propertyStore );
        when( store.getNodeStore() ).thenReturn( nodeStore );
        when( store.getRelationshipStore() ).thenReturn( relationshipStore );
    }

    private static NodeRecord missingNode()
    {
        return new NodeRecord( -1, false, -1, -1 );
    }

    private static NodeRecord createdNode()
    {
        NodeRecord record = new NodeRecord( 2, false, -1, -1 );
        record.setInUse( true );
        record.setCreated();
        return record;
    }

    private static NodeRecord inUseNode()
    {
        NodeRecord record = new NodeRecord( 1, false, -1, -1 );
        record.setInUse( true );
        return record;
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

    @Test
    public void shouldExecuteCommandsInTheSameOrderRegardlessOfItBeingRecoveredOrNot() throws Exception
    {
        // Given
        TransactionRecordState tx = injectAllPossibleCommands();

        // When
        PhysicalTransactionRepresentation commands = transactionRepresentationOf( tx );

        // Then
        final OrderVerifyingCommandHandler orderVerifyingCommandHandler = new OrderVerifyingCommandHandler();
        commands.accept( element -> ((Command)element).handle( orderVerifyingCommandHandler ) );
    }

    private PhysicalTransactionRepresentation transactionRepresentationOf( TransactionRecordState tx )
            throws TransactionFailureException
    {
        List<StorageCommand> commands = new ArrayList<>();
        tx.extractCommands( commands );
        return new PhysicalTransactionRepresentation( commands );
    }

    private TransactionRecordState injectAllPossibleCommands()
    {
        RecordChangeSet recordChangeSet = mock( RecordChangeSet.class );

        RecordChanges<Integer,LabelTokenRecord,Void> labelTokenChanges = mock( RecordChanges.class );
        RecordChanges<Integer,RelationshipTypeTokenRecord,Void> relationshipTypeTokenChanges =
                mock( RecordChanges.class );
        RecordChanges<Integer,PropertyKeyTokenRecord,Void> propertyKeyTokenChanges = mock( RecordChanges.class );
        RecordChanges<Long,NodeRecord,Void> nodeRecordChanges = mock( RecordChanges.class );
        RecordChanges<Long,RelationshipRecord,Void> relationshipRecordChanges = mock( RecordChanges.class );
        RecordChanges<Long,PropertyRecord,PrimitiveRecord> propertyRecordChanges = mock( RecordChanges.class );
        RecordChanges<Long,RelationshipGroupRecord,Integer> relationshipGroupChanges = mock( RecordChanges.class );
        RecordChanges<Long,SchemaRecord,SchemaRule> schemaRuleChanges = mock( RecordChanges.class );

        when( recordChangeSet.getLabelTokenChanges() ).thenReturn( labelTokenChanges );
        when( recordChangeSet.getRelationshipTypeTokenChanges() ).thenReturn( relationshipTypeTokenChanges );
        when( recordChangeSet.getPropertyKeyTokenChanges() ).thenReturn( propertyKeyTokenChanges );
        when( recordChangeSet.getNodeRecords() ).thenReturn( nodeRecordChanges );
        when( recordChangeSet.getRelRecords() ).thenReturn( relationshipRecordChanges );
        when( recordChangeSet.getPropertyRecords() ).thenReturn( propertyRecordChanges );
        when( recordChangeSet.getRelGroupRecords() ).thenReturn( relationshipGroupChanges );
        when( recordChangeSet.getSchemaRuleChanges() ).thenReturn( schemaRuleChanges );

        List<RecordProxy<Long,NodeRecord,Void>> nodeChanges = new LinkedList<>();

        RecordChange<Long,NodeRecord,Void> deletedNode = mock( RecordChange.class );
        when( deletedNode.getBefore() ).thenReturn( inUseNode() );
        when( deletedNode.forReadingLinkage() ).thenReturn( missingNode() );
        nodeChanges.add( deletedNode );

        RecordChange<Long,NodeRecord,Void> createdNode = mock( RecordChange.class );
        when( createdNode.getBefore() ).thenReturn( missingNode() );
        when( createdNode.forReadingLinkage() ).thenReturn( createdNode() );
        nodeChanges.add( createdNode );

        RecordChange<Long,NodeRecord,Void> updatedNode = mock( RecordChange.class );
        when( updatedNode.getBefore() ).thenReturn( inUseNode() );
        when( updatedNode.forReadingLinkage() ).thenReturn( inUseNode() );
        nodeChanges.add( updatedNode );

        when( nodeRecordChanges.changes() ).thenReturn( nodeChanges );
        when( nodeRecordChanges.changeSize() ).thenReturn( 3 );
        when( recordChangeSet.changeSize() ).thenReturn( 3 );

        when( labelTokenChanges.changes() )
                .thenReturn( Collections.<RecordProxy<Integer,LabelTokenRecord,Void>>emptyList() );
        when( relationshipTypeTokenChanges.changes() ).thenReturn(
                Collections.<RecordProxy<Integer,RelationshipTypeTokenRecord,Void>>emptyList() );
        when( propertyKeyTokenChanges.changes() )
                .thenReturn( Collections.<RecordProxy<Integer,PropertyKeyTokenRecord,Void>>emptyList() );
        when( relationshipRecordChanges.changes() )
                .thenReturn( Collections.<RecordProxy<Long,RelationshipRecord,Void>>emptyList() );
        when( propertyRecordChanges.changes() )
                .thenReturn( Collections.<RecordProxy<Long,PropertyRecord,PrimitiveRecord>>emptyList() );
        when( relationshipGroupChanges.changes() ).thenReturn(
                Collections.<RecordProxy<Long,RelationshipGroupRecord,Integer>>emptyList() );
        when( schemaRuleChanges.changes() ).thenReturn(
                Collections.<RecordProxy<Long,SchemaRecord,SchemaRule>>emptyList() );

        NeoStores neoStores = mock( NeoStores.class );
        when( neoStores.getNodeStore() ).thenReturn( mock( NodeStore.class ) );
        when( neoStores.getRelationshipGroupStore() ).thenReturn( mock( RelationshipGroupStore.class ) );
        when( neoStores.getRelationshipStore() ).thenReturn( mock( RelationshipStore.class ) );

        return new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), recordChangeSet,
                0, null, null, null, null, null );
    }

    private static class RecordingPropertyStore extends PropertyStore
    {
        private final AtomicReference<List<String>> currentRecording;

        public RecordingPropertyStore( AtomicReference<List<String>> currentRecording )
        {
            super( null, Config.empty(), null, null, NullLogProvider.getInstance(), null, null, null,
                    Standard.LATEST_RECORD_FORMATS );
            this.currentRecording = currentRecording;
        }

        @Override
        public void updateRecord( PropertyRecord record )
        {
            currentRecording.get().add( commandActionToken( record ) + " property" );
        }

        @Override
        protected void checkStorage( boolean createIfNotExists )
        {
        }

        @Override
        protected void loadStorage()
        {
        }
    }

    private static class RecordingNodeStore extends NodeStore
    {
        private final AtomicReference<List<String>> currentRecording;

        public RecordingNodeStore( AtomicReference<List<String>> currentRecording )
        {
            super( null, Config.empty(), null, null, NullLogProvider.getInstance(), null, Standard.LATEST_RECORD_FORMATS );
            this.currentRecording = currentRecording;
        }

        @Override
        public void updateRecord( NodeRecord record )
        {
            currentRecording.get().add( commandActionToken( record ) + " node" );
        }

        @Override
        protected void checkStorage( boolean createIfNotExists )
        {
        }

        @Override
        protected void loadStorage()
        {
        }

        @Override
        public NodeRecord getRecord( long id, NodeRecord record, RecordLoad mode )
        {
            record.initialize( true, -1, false, -1, 0 );
            record.setId( id );
            return record;
        }
    }

    private static class RecordingRelationshipStore extends RelationshipStore
    {
        private final AtomicReference<List<String>> currentRecording;

        public RecordingRelationshipStore( AtomicReference<List<String>> currentRecording )
        {
            super( null, Config.empty(), null, null, NullLogProvider.getInstance(), Standard.LATEST_RECORD_FORMATS );
            this.currentRecording = currentRecording;
        }

        @Override
        public void updateRecord( RelationshipRecord record )
        {
            currentRecording.get().add( commandActionToken( record ) + " relationship" );
        }

        @Override
        protected void checkStorage( boolean createIfNotExists )
        {
        }

        @Override
        protected void loadStorage()
        {
        }
    }

    private static class OrderVerifyingCommandHandler extends CommandVisitor.Adapter
    {
        private boolean nodeVisited;

        // Commands should appear in this order
        private boolean updated;
        private boolean deleted;

        @Override
        public boolean visitNodeCommand( NodeCommand command ) throws IOException
        {
            if ( !nodeVisited )
            {
                updated = false;
                deleted = false;
            }
            nodeVisited = true;

            switch ( command.getMode() )
            {
            case CREATE:
                assertFalse( updated );
                assertFalse( deleted );
                break;
            case UPDATE:
                updated = true;
                assertFalse( deleted );
                break;
            case DELETE:
                deleted = true;
                break;
            default:
                throw new IllegalStateException( "Unknown command mode: " + command.getMode() );
            }
            return false;
        }
    }
}
