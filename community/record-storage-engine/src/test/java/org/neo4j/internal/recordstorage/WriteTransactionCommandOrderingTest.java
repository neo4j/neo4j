/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.recordstorage.RecordChanges.RecordChange;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.RelationshipGroupStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.record.LabelTokenRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyKeyTokenRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.store.record.RelationshipTypeTokenRecord;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.storageengine.api.CommandsToApply;
import org.neo4j.storageengine.api.StorageCommand;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class WriteTransactionCommandOrderingTest
{
    private static NodeRecord missingNode()
    {
        return new NodeRecord( -1 ).initialize( false, -1, false, -1, 0 );
    }

    private static NodeRecord createdNode()
    {
        NodeRecord record = new NodeRecord( 2 ).initialize(  false, -1, false, -1, 0 );
        record.setInUse( true );
        record.setCreated();
        return record;
    }

    private static NodeRecord inUseNode()
    {
        NodeRecord record = new NodeRecord( 1 ).initialize( false, -1, false, -1, 0 );
        record.setInUse( true );
        return record;
    }

    @Test
    void shouldExecuteCommandsInTheSameOrderRegardlessOfItBeingRecoveredOrNot() throws Exception
    {
        // Given
        TransactionRecordState tx = injectAllPossibleCommands();

        // When
        CommandsToApply commands = transactionRepresentationOf( tx );

        // Then
        final OrderVerifyingCommandHandler orderVerifyingCommandHandler = new OrderVerifyingCommandHandler();
        commands.accept( element -> ((Command)element).handle( orderVerifyingCommandHandler ) );
    }

    private CommandsToApply transactionRepresentationOf( TransactionRecordState tx )
            throws TransactionFailureException
    {
        List<StorageCommand> commands = new ArrayList<>();
        tx.extractCommands( commands );
        return new GroupOfCommands( commands.toArray( new StorageCommand[0] ) );
    }

    private TransactionRecordState injectAllPossibleCommands()
    {
        RecordChangeSet recordChangeSet = mock( RecordChangeSet.class );

        RecordChanges<LabelTokenRecord,Void> labelTokenChanges = mock( RecordChanges.class );
        RecordChanges<RelationshipTypeTokenRecord,Void> relationshipTypeTokenChanges = mock( RecordChanges.class );
        RecordChanges<PropertyKeyTokenRecord,Void> propertyKeyTokenChanges = mock( RecordChanges.class );
        RecordChanges<NodeRecord,Void> nodeRecordChanges = mock( RecordChanges.class );
        RecordChanges<RelationshipRecord,Void> relationshipRecordChanges = mock( RecordChanges.class );
        RecordChanges<PropertyRecord,PrimitiveRecord> propertyRecordChanges = mock( RecordChanges.class );
        RecordChanges<RelationshipGroupRecord,Integer> relationshipGroupChanges = mock( RecordChanges.class );
        RecordChanges<SchemaRecord, SchemaRule> schemaRuleChanges = mock( RecordChanges.class );

        when( recordChangeSet.getLabelTokenChanges() ).thenReturn( labelTokenChanges );
        when( recordChangeSet.getRelationshipTypeTokenChanges() ).thenReturn( relationshipTypeTokenChanges );
        when( recordChangeSet.getPropertyKeyTokenChanges() ).thenReturn( propertyKeyTokenChanges );
        when( recordChangeSet.getNodeRecords() ).thenReturn( nodeRecordChanges );
        when( recordChangeSet.getRelRecords() ).thenReturn( relationshipRecordChanges );
        when( recordChangeSet.getPropertyRecords() ).thenReturn( propertyRecordChanges );
        when( recordChangeSet.getRelGroupRecords() ).thenReturn( relationshipGroupChanges );
        when( recordChangeSet.getSchemaRuleChanges() ).thenReturn( schemaRuleChanges );

        List<RecordProxy<NodeRecord,Void>> nodeChanges = new LinkedList<>();

        RecordChange<NodeRecord,Void> deletedNode = mock( RecordChange.class );
        when( deletedNode.getBefore() ).thenReturn( inUseNode() );
        when( deletedNode.forReadingLinkage() ).thenReturn( missingNode() );
        nodeChanges.add( deletedNode );

        RecordChange<NodeRecord,Void> createdNode = mock( RecordChange.class );
        when( createdNode.getBefore() ).thenReturn( missingNode() );
        when( createdNode.forReadingLinkage() ).thenReturn( createdNode() );
        nodeChanges.add( createdNode );

        RecordChange<NodeRecord,Void> updatedNode = mock( RecordChange.class );
        when( updatedNode.getBefore() ).thenReturn( inUseNode() );
        when( updatedNode.forReadingLinkage() ).thenReturn( inUseNode() );
        nodeChanges.add( updatedNode );

        when( nodeRecordChanges.changes() ).thenReturn( nodeChanges );
        when( nodeRecordChanges.changeSize() ).thenReturn( 3 );
        when( recordChangeSet.changeSize() ).thenReturn( 3 );

        when( labelTokenChanges.changes() ).thenReturn( Collections.emptyList() );
        when( relationshipTypeTokenChanges.changes() ).thenReturn( Collections.emptyList() );
        when( propertyKeyTokenChanges.changes() ).thenReturn( Collections.emptyList() );
        when( relationshipRecordChanges.changes() ).thenReturn( Collections.emptyList() );
        when( propertyRecordChanges.changes() ).thenReturn( Collections.emptyList() );
        when( relationshipGroupChanges.changes() ).thenReturn( Collections.emptyList() );
        when( schemaRuleChanges.changes() ).thenReturn( Collections.emptyList() );

        NeoStores neoStores = mock( NeoStores.class );
        NodeStore store = mock( NodeStore.class );
        when( neoStores.getNodeStore() ).thenReturn( store );
        RelationshipGroupStore relationshipGroupStore = mock( RelationshipGroupStore.class );
        when( neoStores.getRelationshipGroupStore() ).thenReturn( relationshipGroupStore );
        RelationshipStore relationshipStore = mock( RelationshipStore.class );
        when( neoStores.getRelationshipStore() ).thenReturn( relationshipStore );

        return new TransactionRecordState( neoStores, mock( IntegrityValidator.class ), recordChangeSet,
                0, null, null, null, null, null, NULL, INSTANCE );
    }

    private static class OrderVerifyingCommandHandler extends CommandVisitor.Adapter
    {
        private boolean nodeVisited;

        // Commands should appear in this order
        private boolean updated;
        private boolean deleted;

        @Override
        public boolean visitNodeCommand( NodeCommand command )
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
                Assertions.assertFalse( updated );
                Assertions.assertFalse( deleted );
                break;
            case UPDATE:
                updated = true;
                Assertions.assertFalse( deleted );
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
