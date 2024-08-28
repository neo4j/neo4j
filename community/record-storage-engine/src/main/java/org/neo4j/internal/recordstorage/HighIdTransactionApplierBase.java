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

import static java.lang.Math.max;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.neo4j.internal.recordstorage.Command.BaseCommand;
import org.neo4j.internal.recordstorage.Command.LabelTokenCommand;
import org.neo4j.internal.recordstorage.Command.NodeCommand;
import org.neo4j.internal.recordstorage.Command.PropertyCommand;
import org.neo4j.internal.recordstorage.Command.PropertyKeyTokenCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipGroupCommand;
import org.neo4j.internal.recordstorage.Command.RelationshipTypeTokenCommand;
import org.neo4j.internal.recordstorage.Command.SchemaRuleCommand;
import org.neo4j.internal.recordstorage.Command.TokenCommand;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.SchemaStore;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.TokenRecord;

/**
 * High ids map is built up over the lifetime of this class and is expected to be handled by subclasses on close.
 */
public class HighIdTransactionApplierBase extends TransactionApplier.Adapter {
    private final NeoStores neoStores;
    protected final Map<RecordStore<?>, HighId> highIds = new HashMap<>();

    public HighIdTransactionApplierBase(NeoStores neoStores) {
        this.neoStores = neoStores;
    }

    @Override
    public boolean visitNodeCommand(NodeCommand command) {
        NodeStore nodeStore = neoStores.getNodeStore();
        track(nodeStore, command);
        track(nodeStore.getDynamicLabelStore(), command.getAfter().getDynamicLabelRecords());
        return false;
    }

    @Override
    public boolean visitRelationshipCommand(RelationshipCommand command) {
        track(neoStores.getRelationshipStore(), command);
        return false;
    }

    @Override
    public boolean visitPropertyCommand(PropertyCommand command) {
        PropertyStore propertyStore = neoStores.getPropertyStore();
        track(propertyStore, command);
        for (PropertyBlock block : command.getAfter().propertyBlocks()) {
            switch (block.getType()) {
                case STRING -> track(propertyStore.getStringStore(), block.getValueRecords());
                case ARRAY -> track(propertyStore.getArrayStore(), block.getValueRecords());
                default -> {
                    // Not needed, no dynamic records then
                }
            }
        }
        return false;
    }

    @Override
    public boolean visitRelationshipGroupCommand(RelationshipGroupCommand command) {
        track(neoStores.getRelationshipGroupStore(), command);
        return false;
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand(RelationshipTypeTokenCommand command) {
        trackToken(neoStores.getRelationshipTypeTokenStore(), command);
        return false;
    }

    @Override
    public boolean visitLabelTokenCommand(LabelTokenCommand command) {
        trackToken(neoStores.getLabelTokenStore(), command);
        return false;
    }

    @Override
    public boolean visitPropertyKeyTokenCommand(PropertyKeyTokenCommand command) {
        trackToken(neoStores.getPropertyKeyTokenStore(), command);
        return false;
    }

    @Override
    public boolean visitSchemaRuleCommand(SchemaRuleCommand command) {
        SchemaStore schemaStore = neoStores.getSchemaStore();
        track(schemaStore, command.getAfter());
        return false;
    }

    private void track(RecordStore<?> store, AbstractBaseRecord record) {
        long id = max(record.getId(), record.requiresSecondaryUnit() ? record.getSecondaryUnitId() : -1);
        HighId highId = highIds.get(store);
        if (highId == null) {
            highIds.put(store, new HighId(id));
        } else {
            highId.track(id);
        }
    }

    private <RECORD extends AbstractBaseRecord> void track(RecordStore<RECORD> store, BaseCommand<RECORD> command) {
        track(store, command.getAfter());
    }

    private void track(RecordStore<?> store, Collection<? extends AbstractBaseRecord> records) {
        for (AbstractBaseRecord record : records) {
            track(store, record);
        }
    }

    private <RECORD extends TokenRecord> void trackToken(
            TokenStore<RECORD> tokenStore, TokenCommand<RECORD> tokenCommand) {
        track(tokenStore, tokenCommand.getAfter());
        track(tokenStore.getNameStore(), tokenCommand.getAfter().getNameRecords());
    }

    protected static class HighId {
        protected long id;

        HighId(long id) {
            this.id = id;
        }

        void track(long id) {
            if (id > this.id) {
                this.id = id;
            }
        }
    }
}
