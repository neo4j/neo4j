/**
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
package org.neo4j.kernel.impl.nioneo.xa.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.api.exceptions.index.IndexActivationFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexNotFoundKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexPopulationFailedKernelException;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.nioneo.store.DynamicRecord;
import org.neo4j.kernel.impl.nioneo.store.IndexRule;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.RelationshipRecord;
import org.neo4j.kernel.impl.nioneo.store.UniquenessConstraintRule;
import org.neo4j.kernel.impl.transaction.xaframework.XaCommand;

public class NeoXaCommandExecutor implements XaCommandExecutor
{
    private final NeoCommandExecutor executingVisitor;
    private NeoStore store;
    private IndexingService indexes;

    public NeoXaCommandExecutor( NeoStore store, IndexingService indexes )
    {
        executingVisitor = new NeoCommandExecutor();
        this.store = store;
        this.indexes = indexes;
    }

    @Override
    public void execute( XaCommand command ) throws IOException
    {
        ((Command) command).accept( executingVisitor );
    }

    private class NeoCommandExecutor implements NeoCommandVisitor
    {
        @Override
        public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
        {
            store.getNodeStore().updateRecord( command.getAfter() );

            // Dynamic Label Records
            Collection<DynamicRecord> toUpdate = new ArrayList<>( command.getAfter().getDynamicLabelRecords() );
            // the dynamic label records that exist in before, but not in after should be deleted.
            Set<Long> idsToRemove = new HashSet<>();
            for ( DynamicRecord record : command.getBefore().getDynamicLabelRecords() )
            {
                idsToRemove.add( record.getId() );
            }
            for ( DynamicRecord record : command.getAfter().getDynamicLabelRecords() )
            {
                idsToRemove.remove( record.getId() );
            }
            for ( long id : idsToRemove )
            {
                toUpdate.add( new DynamicRecord( id ) );
            }
            store.getNodeStore().updateDynamicLabelRecords( toUpdate );
            return true;
        }

        @Override
        public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
        {
            RelationshipRecord record = command.getRecord();
            if ( command.isRecovered() && !record.inUse() )
            {
                /*
                 * If read from a log (either on recovery or HA) then all the fields but for the Id are -1. If the
                 * record is deleted, then we'll need to invalidate the cache and patch the node's relationship chains.
                 * Therefore, we need to read the record from the store. This is not too expensive, since the window
                 * will be either in memory or will soon be anyway and we are just saving the write the trouble.
                 */
                 command.setBefore( store.getRelationshipStore().forceGetRaw( record.getId() ) );
            }
            store.getRelationshipStore().updateRecord( record );
            return true;
        }

        @Override
        public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
        {
            store.getPropertyStore().updateRecord( command.getAfter() );
            return true;
        }

        @Override
        public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
        {
            if ( command.isRecovered() )
            {
                store.getRelationshipGroupStore().updateRecord( command.getRecord(), true );
            }
            else
            {
                store.getRelationshipGroupStore().updateRecord( command.getRecord() );
            }
            return true;
        }

        @Override
        public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command ) throws
                IOException
        {
            store.getRelationshipTypeTokenStore().updateRecord( command.getRecord() );
            return true;
        }

        @Override
        public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
        {
            store.getLabelTokenStore().updateRecord( command.getRecord() );
            return true;
        }

        @Override
        public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
        {
            store.getPropertyStore().getPropertyKeyTokenStore().updateRecord( command.getRecord() );
            return true;
        }

        @Override
        public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
        {
            for ( DynamicRecord record : command.getRecordsAfter() )
            {
                store.getSchemaStore().updateRecord( record );
            }

            if ( command.getSchemaRule() instanceof IndexRule )
            {
                switch ( command.getMode() )
                {
                    case UPDATE:
                        // Shouldn't we be more clear about that we are waiting for an index to come online here?
                        // right now we just assume that an update to index records means wait for it to be online.
                        if ( ((IndexRule) command.getSchemaRule()).isConstraintIndex() )
                        {
                            try
                            {
                                indexes.activateIndex( command.getSchemaRule().getId() );
                            }
                            catch ( IndexNotFoundKernelException | IndexActivationFailedKernelException |
                                    IndexPopulationFailedKernelException e )
                            {
                                throw new IllegalStateException( "Unable to enable constraint, backing index is not online.", e );
                            }
                        }
                        break;
                    case CREATE:
                        indexes.createIndex( (IndexRule) command.getSchemaRule() );
                        break;
                    case DELETE:
                        indexes.dropIndex( (IndexRule) command.getSchemaRule() );
                        break;
                    default:
                        throw new IllegalStateException( command.getMode().name() );
                }
            }

            if( command.getSchemaRule() instanceof UniquenessConstraintRule )
            {
                switch ( command.getMode() )
                {
                    case UPDATE:
                    case CREATE:
                        store.setLatestConstraintIntroducingTx( command.getTxId() );
                        break;
                    case DELETE:
                        break;
                    default:
                        throw new IllegalStateException( command.getMode().name() );
                }
            }
            return true;
        }

        @Override
        public boolean visitNeoStoreCommand( Command.NeoStoreCommand command ) throws IOException
        {
            store.setGraphNextProp( command.getRecord().getNextProp() );
            return true;
        }
    }
}
