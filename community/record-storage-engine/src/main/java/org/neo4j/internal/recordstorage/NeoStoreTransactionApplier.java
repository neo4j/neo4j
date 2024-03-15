/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.recordstorage;

import static org.neo4j.internal.recordstorage.RecordCursorTypes.GROUP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.LABEL_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_KEY_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.REL_TYPE_TOKEN_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.SCHEMA_CURSOR;

import org.neo4j.internal.recordstorage.Command.BaseCommand;
import org.neo4j.internal.schema.SchemaRule;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.storageengine.api.CommandVersion;
import org.neo4j.storageengine.api.TransactionApplicationMode;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;
import org.neo4j.token.api.NamedToken;

public class NeoStoreTransactionApplier extends TransactionApplier.Adapter {
    private final TransactionApplicationMode mode;
    private final CommandVersion version;
    private final NeoStores neoStores;
    private final CacheAccessBackDoor cacheAccess;
    private final IdUpdateListener idUpdateListener;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;

    public NeoStoreTransactionApplier(
            TransactionApplicationMode mode,
            CommandVersion version,
            NeoStores neoStores,
            CacheAccessBackDoor cacheAccess,
            BatchContext batchContext,
            CursorContext cursorContext,
            StoreCursors storeCursors) {
        this.mode = mode;
        this.version = version;
        this.neoStores = neoStores;
        this.cacheAccess = cacheAccess;
        this.idUpdateListener = batchContext.getIdUpdateListener();
        this.cursorContext = cursorContext;
        this.storeCursors = storeCursors;
    }

    @Override
    public boolean visitNodeCommand(Command.NodeCommand command) {
        // update store
        updateStore(neoStores.getNodeStore(), command, NODE_CURSOR);
        return false;
    }

    @Override
    public boolean visitRelationshipCommand(Command.RelationshipCommand command) {
        updateStore(neoStores.getRelationshipStore(), command, RELATIONSHIP_CURSOR);
        return false;
    }

    @Override
    public boolean visitPropertyCommand(Command.PropertyCommand command) {
        updateStore(neoStores.getPropertyStore(), command, PROPERTY_CURSOR);
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand(Command.RelationshipGroupCommand command) {
        updateStore(neoStores.getRelationshipGroupStore(), command, GROUP_CURSOR);
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand(Command.RelationshipTypeTokenCommand command) {
        updateStore(neoStores.getRelationshipTypeTokenStore(), command, REL_TYPE_TOKEN_CURSOR);
        if (!mode.isReverseStep()) {
            cacheAccess.addRelationshipTypeToken(
                    getTokenFromTokenCommand(command), mode != TransactionApplicationMode.RECOVERY);
        }
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand(Command.LabelTokenCommand command) {
        updateStore(neoStores.getLabelTokenStore(), command, LABEL_TOKEN_CURSOR);
        if (!mode.isReverseStep()) {
            cacheAccess.addLabelToken(getTokenFromTokenCommand(command), mode != TransactionApplicationMode.RECOVERY);
        }
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand(Command.PropertyKeyTokenCommand command) {
        updateStore(neoStores.getPropertyKeyTokenStore(), command, PROPERTY_KEY_TOKEN_CURSOR);
        if (!mode.isReverseStep()) {
            cacheAccess.addPropertyKeyToken(
                    getTokenFromTokenCommand(command), mode != TransactionApplicationMode.RECOVERY);
        }
        return false;
    }

    private NamedToken getTokenFromTokenCommand(Command.TokenCommand<?> command) {
        var data = AbstractDynamicStore.getFullByteArrayFromHeavyRecords(
                command.getAfter().getNameRecords(), PropertyType.STRING);
        String name = PropertyStore.decodeString(data);
        return new NamedToken(name, command.tokenId(), command.isInternal());
    }

    @Override
    public boolean visitSchemaRuleCommand(Command.SchemaRuleCommand command) {
        // schema rules. Execute these after generating the property updates so. If executed
        // before and we've got a transaction that sets properties/labels as well as creating an index
        // we might end up with this corner-case:
        // 1) index rule created and index population job started
        // 2) index population job processes some nodes, but doesn't complete
        // 3) we gather up property updates and send those to the indexes. The newly created population
        //    job might get those as updates
        // 4) the population job will apply those updates as added properties, and might end up with duplicate
        //    entries for the same property
        updateStore(neoStores.getSchemaStore(), command, SCHEMA_CURSOR);
        SchemaRule schemaRule = command.getSchemaRule();
        onSchemaRuleChange(command.getMode(), command.getKey(), schemaRule);
        return false;
    }

    private void onSchemaRuleChange(Command.Mode commandMode, long schemaRuleId, SchemaRule schemaRule) {
        if (commandMode == Command.Mode.DELETE) {
            cacheAccess.removeSchemaRuleFromCache(schemaRuleId);
        } else {
            cacheAccess.addSchemaRule(schemaRule);
        }
    }

    private <RECORD extends AbstractBaseRecord> void updateStore(
            CommonAbstractStore<RECORD, ?> store, BaseCommand<RECORD> command, CursorType cursorType) {
        try (var cursor = storeCursors.writeCursor(cursorType)) {
            store.updateRecord(
                    selectRecordByCommandVersion(command), idUpdateListener, cursor, cursorContext, storeCursors);
        }
    }

    private <RECORD extends AbstractBaseRecord> RECORD selectRecordByCommandVersion(BaseCommand<RECORD> command) {
        return switch (version) {
            case BEFORE -> command.getBefore();
            case AFTER -> command.getAfter();
        };
    }
}
