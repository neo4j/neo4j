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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.kernel.impl.api.TransactionApplier;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.kernel.impl.transaction.command.Command.BaseCommand;
import org.neo4j.kernel.impl.transaction.command.Command.LabelTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyKeyTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipGroupCommand;
import org.neo4j.kernel.impl.transaction.command.Command.RelationshipTypeTokenCommand;
import org.neo4j.kernel.impl.transaction.command.Command.SchemaRuleCommand;
import org.neo4j.kernel.impl.transaction.command.Command.TokenCommand;
import org.neo4j.storageengine.api.Token;

import static java.lang.Math.max;

public class HighIdTransactionApplier extends TransactionApplier.Adapter
{
    private final NeoStores neoStores;
    private final Map<RecordStore<?>,HighId> highIds = new HashMap<>();

    public HighIdTransactionApplier( NeoStores neoStores )
    {
        this.neoStores = neoStores;
    }

    @Override
    public boolean visitNodeCommand( NodeCommand command )
    {
        NodeStore nodeStore = neoStores.getNodeStore();
        track( nodeStore, command );
        track( nodeStore.getDynamicLabelStore(), command.getAfter().getDynamicLabelRecords() );
        return false;
    }

    @Override
    public boolean visitRelationshipCommand( RelationshipCommand command )
    {
        track( neoStores.getRelationshipStore(), command );
        return false;
    }

    @Override
    public boolean visitPropertyCommand( PropertyCommand command )
    {
        PropertyStore propertyStore = neoStores.getPropertyStore();
        track( propertyStore, command );
        for ( PropertyBlock block : command.getAfter() )
        {
            switch ( block.getType() )
            {
            case STRING:
                track( propertyStore.getStringStore(), block.getValueRecords() );
                break;
            case ARRAY:
                track( propertyStore.getArrayStore(), block.getValueRecords() );
                break;
            default:
                // Not needed, no dynamic records then
                break;
            }
        }
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand( RelationshipGroupCommand command )
    {
        track( neoStores.getRelationshipGroupStore(), command );
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( RelationshipTypeTokenCommand command )
    {
        trackToken( neoStores.getRelationshipTypeTokenStore(), command );
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand( LabelTokenCommand command )
    {
        trackToken( neoStores.getLabelTokenStore(), command );
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( PropertyKeyTokenCommand command )
    {
        trackToken( neoStores.getPropertyKeyTokenStore(), command );
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand( SchemaRuleCommand command )
    {
        SchemaStore schemaStore = neoStores.getSchemaStore();
        for ( DynamicRecord record : command.getRecordsAfter() )
        {
            track( schemaStore, record );
        }
        return false;
    }

    @Override
    public void close()
    {
        // Notifies the stores about the recovered ids and will bump those high ids atomically if
        // they surpass the current high ids
        for ( Map.Entry<RecordStore<?>,HighId> highId : highIds.entrySet() )
        {
            highId.getKey().setHighestPossibleIdInUse( highId.getValue().id );
        }
    }

    private void track( RecordStore<?> store, AbstractBaseRecord record )
    {
        long id = max( record.getId(), record.requiresSecondaryUnit() ? record.getSecondaryUnitId() : -1 );
        HighId highId = highIds.get( store );
        if ( highId == null )
        {
            highIds.put( store, new HighId( id ) );
        }
        else
        {
            highId.track( id );
        }
    }

    private <RECORD extends AbstractBaseRecord> void track( RecordStore<RECORD> store, BaseCommand<RECORD> command )
    {
        track( store, command.getAfter() );
    }

    private void track( RecordStore<?> store, Collection<? extends AbstractBaseRecord> records )
    {
        for ( AbstractBaseRecord record : records )
        {
            track( store, record );
        }
    }

    private <RECORD extends TokenRecord, TOKEN extends Token>
    void trackToken( TokenStore<RECORD, TOKEN> tokenStore, TokenCommand<RECORD> tokenCommand )
    {
        track( tokenStore, tokenCommand.getAfter() );
        track( tokenStore.getNameStore(), tokenCommand.getAfter().getNameRecords() );
    }

    private static class HighId
    {
        private long id;

        HighId( long id )
        {
            this.id = id;
        }

        void track( long id )
        {
            if ( id > this.id )
            {
                this.id = id;
            }
        }
    }
}
