/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.neo4j.helpers.Function;
import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCountsCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCountsCommand;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryByteCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommand;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryVersion;
import org.neo4j.kernel.impl.transaction.state.PropertyDeleter;
import org.neo4j.kernel.impl.transaction.state.PropertyTraverser;
import org.neo4j.kernel.impl.transaction.state.RecordAccess.RecordProxy;
import org.neo4j.kernel.impl.transaction.state.RecordChangeSet;
import org.neo4j.kernel.impl.transaction.state.RelationshipCreator;
import org.neo4j.kernel.impl.transaction.state.RelationshipDeleter;
import org.neo4j.kernel.impl.transaction.state.RelationshipGroupGetter;
import org.neo4j.kernel.impl.transaction.state.RelationshipLocker;

public class DenseNodeTransactionTranslator implements Function<List<LogEntry>,List<LogEntry>>
{
    private final NeoStore neoStore;
    private final List<LogEntryCommand> commands = new LinkedList<>();
    private final RelationshipGroupGetter groupGetter;
    private final RelationshipCreator relationshipCreator;
    private final RelationshipDeleter deleter;
    private final DenseNodeTransactionTranslator.TranslatingNeoCommandVisitor commandVisitor =
            new TranslatingNeoCommandVisitor();
    private RecordChangeSet recordChangeSet;

    public DenseNodeTransactionTranslator( NeoStore neoStore )
    {
        this.neoStore = neoStore;

        groupGetter = new RelationshipGroupGetter( neoStore.getRelationshipGroupStore() );
        relationshipCreator = new RelationshipCreator( new RelationshipLocker()
        {
            @Override
            public void getWriteLock( long relId ) throws AcquireLockTimeoutException
            {
                return; // no locking when applying transactions
            }
        }, groupGetter, 1 );

        deleter = new RelationshipDeleter( new RelationshipLocker()
        {
            @Override
            public void getWriteLock( long relId ) throws AcquireLockTimeoutException
            {
                return; // no locking when applying transactions
            }
        }, groupGetter, new PropertyDeleter(
                neoStore.getPropertyStore(), new PropertyTraverser() ) );
    }

    @Override
    public synchronized List<LogEntry> apply( List<LogEntry> from )
    {
        // Setup the state for this translation
        this.recordChangeSet = new RecordChangeSet( neoStore );
        commands.clear();
        List<LogEntry> result = new ArrayList<>( from.size() );

        LogEntry commit = null;
        LogEntry prepare = null;
        LogEntry done = null;
        for ( LogEntry logEntry : from )
        {
            if ( logEntry.getVersion() == LogEntryVersion.CURRENT )
            {
                throw new RuntimeException( "crap" );
            }

            switch ( logEntry.getType() )
            {
            case LogEntryByteCodes.TX_START:
                result.add( logEntry );
                break;
            case LogEntryByteCodes.TX_1P_COMMIT:
                commit = logEntry;
                break;
            case LogEntryByteCodes.COMMAND:
                try
                {
                    if ( !handleCommand( (LogEntryCommand) logEntry ) )
                    {
                        commands.add( (LogEntryCommand) logEntry );
                    }
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
                break;
            default:
                throw new IllegalStateException( "Log Entry type " + logEntry.getType() + " is not recognizable" );
            }
        }
        translateRecordChangeSetToEntries( result, commands );
        if ( commit != null )
        {
            result.add( commit );
        }
        if ( prepare != null )
        {
            result.add( prepare );
        }
        if ( done != null )
        {
            result.add( done );
        }
        return result;
    }

    private void translateRecordChangeSetToEntries( List<LogEntry> result, List<LogEntryCommand> commands )
    {
        for ( RecordProxy<Long,NodeRecord,Void> nodeChange : recordChangeSet.getNodeRecords().changes() )
        {
            Command.NodeCommand newCommand = new Command.NodeCommand();
            newCommand.init( nodeChange.getBefore(), nodeChange.forChangingData() );
            result.add( new LogEntryCommand( newCommand ) );
        }

        for ( RecordProxy<Long,RelationshipRecord,Void> relChange : recordChangeSet.getRelRecords().changes() )
        {
            Command.RelationshipCommand newCommand = new Command.RelationshipCommand();
            newCommand.init( relChange.forChangingData() );
            result.add( new LogEntryCommand( newCommand ) );
        }

        for ( RecordProxy<Long,RelationshipGroupRecord,Integer> relGroupChange :
                recordChangeSet.getRelGroupRecords().changes() )
        {
            Command.RelationshipGroupCommand newCommand = new Command.RelationshipGroupCommand();
            newCommand.init( relGroupChange.forChangingData() );
            result.add( new LogEntryCommand( newCommand ) );
        }

        for ( RecordProxy<Long,PropertyRecord,PrimitiveRecord> propChange :
                recordChangeSet.getPropertyRecords().changes() )
        {
            Command.PropertyCommand newCommand = new Command.PropertyCommand();
            newCommand.init( propChange.getBefore(), propChange.forChangingData() );
            result.add( new LogEntryCommand( newCommand ) );
        }

        for ( LogEntryCommand commandEntry : commands )
        {
            Command command = commandEntry.getXaCommand();
            if ( command instanceof Command.RelationshipCommand )
            {
                long id = ((Command.RelationshipCommand) command).getRecord().getId();
                if ( recordChangeSet.getRelRecords().getIfLoaded( id ) == null )
                {
                    result.add( commandEntry );
                }
            }
            else if ( command instanceof Command.NodeCommand )
            {
                long id = ((Command.NodeCommand) command).getAfter().getId();
                if ( recordChangeSet.getNodeRecords().getIfLoaded( id ) == null )
                {
                    result.add( commandEntry );
                }
            }
            else
            {
                result.add( commandEntry );
            }
        }
    }

    private boolean handleCommand( LogEntryCommand commandEntry ) throws IOException
    {
        Command command = commandEntry.getXaCommand();
        return command.handle( commandVisitor );
    }

    private class TranslatingNeoCommandVisitor implements NeoCommandHandler
    {
        @Override
        public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
        {
            NodeRecord after = command.getAfter();
            NodeRecord before = neoStore.getNodeStore().loadLightNode( after.getId() );

            if ( after.inUse() && (before == null || !before.inUse()) ) // before either is not in use or does not exist
            {
                translateNodeCreation( command );
            }
            else if ( !after.inUse() && before.inUse() )
            {
                translateNodeDeletion( command );
            }
            else
            {
                if ( after.getNextProp() != before.getNextProp() )
                {
                    translateNodePropertyChange( command );
                }
                if ( after.getLabelField() != before.getLabelField() )
                {
                    translateNodeLabelChange( command );
                }
            }
            return true;
        }

        private void translateNodeDeletion( Command.NodeCommand command )
        {
            recordChangeSet.getNodeRecords().getOrLoad( command.getKey(), null ).forChangingData().setInUse( false );
        }

        private void translateNodeLabelChange( Command.NodeCommand command )
        {
            recordChangeSet.getNodeRecords().getOrLoad( command.getKey(), null )
                           .forChangingData().setLabelField( command.getAfter().getLabelField(),
                    command.getAfter().getDynamicLabelRecords() );
        }

        private void translateNodePropertyChange( Command.NodeCommand command )
        {
            recordChangeSet.getNodeRecords().getOrLoad( command.getKey(), null )
                           .forChangingData().setNextProp( command.getAfter().getNextProp() );
        }

        private void translateNodeCreation( Command.NodeCommand command )
        {
            NodeRecord created = recordChangeSet.getNodeRecords().create( command.getKey(), null ).forChangingData();
            created.copyFrom( command.getAfter() );
            created.setNextRel( Record.NO_NEXT_RELATIONSHIP.intValue() );
            created.setInUse( true );
            recordChangeSet.getNodeRecords().getOrLoad( command.getKey(), null )
                           .forChangingData().setNextProp( command.getAfter().getNextProp() );
        }

        @Override
        public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
        {
            RelationshipRecord after = command.getRecord();
            RelationshipRecord before = neoStore.getRelationshipStore().getLightRel( after.getId() );

            if ( after.inUse() && (before == null || !before.inUse()) ) // before either is not in use or does not exist
            {
                translateRelationshipCreation( command );
            }
            else if ( !after.inUse() && before.inUse() )
            {
                translateRelationshipDeletion( command );
            }
            else if ( after.getNextProp() != before.getNextProp() )
            {
                translateRelationshipPropertyChange( command );
            }
            return true;
        }

        private void translateRelationshipPropertyChange( Command.RelationshipCommand command )
        {
            recordChangeSet.getRelRecords().getOrLoad( command.getKey(), null ).forChangingData()
                           .setNextProp( command.getRecord().getNextProp() );
        }

        @Override
        public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
        {
            PrimitiveRecord additionalData;
            if ( command.getAfter().isNodeSet() )
            {
                additionalData = recordChangeSet.getNodeRecords().getOrLoad( command.getAfter().getNodeId
                        (), null ).forReadingLinkage();
            }
            else
            {
                additionalData = recordChangeSet.getRelRecords().getOrLoad( command.getAfter()
                                                                                   .getRelId(), null )
                                                .forReadingLinkage();

            }
            recordChangeSet.getPropertyRecords().setTo( command.getKey(), command.getAfter(), additionalData );
            return true;
        }

        @Override
        public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command )
        {
            return false;
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command )
        {
            return false;
        }

        @Override
        public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command )
        {
            return false;
        }

        @Override
        public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitNeoStoreCommand( Command.NeoStoreCommand command )
        {
            return false;
        }

        @Override
        public boolean visitIndexAddNodeCommand( IndexCommand.AddNodeCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexAddRelationshipCommand( IndexCommand.AddRelationshipCommand command )
                throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexRemoveCommand( IndexCommand.RemoveCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexDeleteCommand( IndexCommand.DeleteCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexCreateCommand( IndexCommand.CreateCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitIndexDefineCommand( IndexDefineCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public boolean visitNodeCountsCommand( NodeCountsCommand command )
        {
            return false;
        }

        @Override
        public boolean visitRelationshipCountsCommand( RelationshipCountsCommand command ) throws IOException
        {
            return false;
        }

        @Override
        public void apply()
        {
        }

        @Override
        public void close()
        {
        }

        private void translateRelationshipCreation( Command.RelationshipCommand command )
        {
            RelationshipRecord record = command.getRecord();
            relationshipCreator.relationshipCreate( record.getId(), record.getType(), record.getFirstNode(),
                    record.getSecondNode(), recordChangeSet );
            recordChangeSet.getRelRecords().getOrLoad( command.getKey(), null ).forChangingData()
                           .setNextProp( command.getRecord().getNextProp() );
        }

        private void translateRelationshipDeletion( Command.RelationshipCommand command )
        {
            RelationshipRecord record = command.getRecord();
            deleter.relDelete( record.getId(), recordChangeSet );
        }
    }
}
