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

import org.neo4j.internal.recordstorage.Command.BaseCommand;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.IdUpdateListener;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.storageengine.api.CommandVersion;

/**
 * Visits commands targeted towards the {@link NeoStores} and update corresponding stores.
 * What happens in here is what will happen in a "internal" transaction, i.e. a transaction that has been
 * forged in this database, with transaction state, a KernelTransaction and all that and is now committing.
 * <p>
 * For other modes of application, like recovery or external there are other, added functionality, decorated
 * outside this applier.
 */
public class NeoStoreTransactionApplier extends TransactionApplier.Adapter
{
    private final CommandVersion version;
    private final LockGroup lockGroup;
    private final long transactionId;
    private final NeoStores neoStores;
    private final CacheAccessBackDoor cacheAccess;
    private final LockService lockService;
    private final IdUpdateListener idUpdateListener;

    public NeoStoreTransactionApplier( CommandVersion version, NeoStores neoStores, CacheAccessBackDoor cacheAccess, LockService lockService,
            long transactionId, LockGroup lockGroup, IdUpdateListener idUpdateListener )
    {
        this.version = version;
        this.lockGroup = lockGroup;
        this.transactionId = transactionId;
        this.lockService = lockService;
        this.neoStores = neoStores;
        this.cacheAccess = cacheAccess;
        this.idUpdateListener = idUpdateListener;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command )
    {
        // acquire lock
        lockGroup.add( lockService.acquireNodeLock( command.getKey(), LockService.LockType.WRITE_LOCK ) );

        // update store
        updateStore( neoStores.getNodeStore(), command );
        return false;
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command )
    {
        lockGroup.add( lockService.acquireRelationshipLock( command.getKey(), LockService.LockType.WRITE_LOCK ) );

        updateStore( neoStores.getRelationshipStore(), command );
        return false;
    }

    @Override
    public boolean visitPropertyCommand( Command.PropertyCommand command )
    {
        // acquire lock
        if ( command.getNodeId() != -1 )
        {
            lockGroup.add( lockService.acquireNodeLock( command.getNodeId(), LockService.LockType.WRITE_LOCK ) );
        }
        else if ( command.getRelId() != -1 )
        {
            lockGroup.add( lockService.acquireRelationshipLock( command.getRelId(), LockService.LockType.WRITE_LOCK ) );
        }

        updateStore( neoStores.getPropertyStore(), command );
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command )
    {
        updateStore( neoStores.getRelationshipGroupStore(), command );
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command )
    {
        updateStore( neoStores.getRelationshipTypeTokenStore(), command );
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command )
    {
        updateStore( neoStores.getLabelTokenStore(), command );
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command )
    {
        updateStore( neoStores.getPropertyKeyTokenStore(), command );
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command )
    {
        // schema rules. Execute these after generating the property updates so. If executed
        // before and we've got a transaction that sets properties/labels as well as creating an index
        // we might end up with this corner-case:
        // 1) index rule created and index population job started
        // 2) index population job processes some nodes, but doesn't complete
        // 3) we gather up property updates and send those to the indexes. The newly created population
        //    job might get those as updates
        // 4) the population job will apply those updates as added properties, and might end up with duplicate
        //    entries for the same property
        updateStore( neoStores.getSchemaStore(), command );
        SchemaRule schemaRule = command.getSchemaRule();
        boolean isConstraint = command.getAfter().isConstraint();
        onSchemaRuleChange( command.getMode(), command.getKey(), schemaRule, isConstraint );
        return false;
    }

    private void onSchemaRuleChange( Command.Mode commandMode, long schemaRuleId, SchemaRule schemaRule, boolean isConstraint )
    {
        if ( isConstraint )
        {
            switch ( commandMode )
            {
            case UPDATE:
            case CREATE:
                neoStores.getMetaDataStore().setLatestConstraintIntroducingTx( transactionId );
                break;
            case DELETE:
                break;
            default:
                throw new IllegalStateException( commandMode.name() );
            }
        }

        if ( commandMode == Command.Mode.DELETE )
        {
            cacheAccess.removeSchemaRuleFromCache( schemaRuleId );
        }
        else
        {
            cacheAccess.addSchemaRule( schemaRule );
        }
    }

    private <RECORD extends AbstractBaseRecord> void updateStore( CommonAbstractStore<RECORD,?> store, BaseCommand<RECORD> command )
    {
        store.updateRecord( selectRecordByCommandVersion( command ), idUpdateListener );
    }

    private <RECORD extends AbstractBaseRecord> RECORD selectRecordByCommandVersion( BaseCommand<RECORD> command )
    {
        switch ( version )
        {
        case BEFORE:
            return command.getBefore();
        case AFTER:
            return command.getAfter();
        default:
            throw new IllegalArgumentException( "Unexpected command version " + version );
        }
    }
}
