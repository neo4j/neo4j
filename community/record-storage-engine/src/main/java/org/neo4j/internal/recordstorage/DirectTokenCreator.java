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

import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;
import static org.neo4j.kernel.impl.store.StoreType.LABEL_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.PROPERTY_KEY_TOKEN_NAME;
import static org.neo4j.kernel.impl.store.StoreType.RELATIONSHIP_TYPE_TOKEN_NAME;

import java.util.Collection;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicRecordAllocator;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.TokenRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.TokenCreator;

/**
 * Creates tokens directly in the store.
 * @param <R> the type of token record.
 */
public class DirectTokenCreator<R extends TokenRecord> implements TokenCreator {
    private final NeoStores neoStores;
    private final TokenStore<R> store;
    private final CursorContextFactory contextFactory;
    private final MemoryTracker memoryTracker;
    private final IdGenerator storeIdGenerator;
    private final DynamicRecordAllocator dynamicRecordAllocator;

    public DirectTokenCreator(
            NeoStores neoStores,
            TokenStore<R> store,
            DynamicRecordAllocator dynamicRecordAllocator,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        this.neoStores = neoStores;
        this.store = store;
        this.storeIdGenerator = store.getIdGenerator();
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
        this.dynamicRecordAllocator = dynamicRecordAllocator;
    }

    @Override
    public int createToken(String name, boolean internal) throws KernelException {
        int keyId;
        try (CursorContext cursorContext = contextFactory.create("create token");
                StoreCursors storeCursors = new CachedStoreCursors(neoStores, cursorContext);
                PageCursor cursor = store.openPageCursorForWriting(0, cursorContext)) {
            keyId = (int) storeIdGenerator.nextId(cursorContext);
            R record = store.newRecord();
            record.setId(keyId);
            record.setInUse(true);
            record.setInternal(internal);
            record.setCreated();
            Collection<DynamicRecord> keyRecords =
                    store.allocateNameRecords(encodeString(name), dynamicRecordAllocator, cursorContext, memoryTracker);
            record.setNameId((int) Iterables.first(keyRecords).getId());
            record.addNameRecords(keyRecords);
            store.updateRecord(record, cursor, cursorContext, storeCursors);
        }
        return keyId;
    }

    public static TokenCreator directLabelTokenCreator(
            NeoStores neoStores,
            DynamicAllocatorProvider allocatorProvider,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        return new DirectTokenCreator<>(
                neoStores,
                neoStores.getLabelTokenStore(),
                allocatorProvider.allocator(LABEL_TOKEN_NAME),
                contextFactory,
                memoryTracker);
    }

    public static TokenCreator directPropertyKeyTokenCreator(
            NeoStores neoStores,
            DynamicAllocatorProvider allocatorProvider,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        return new DirectTokenCreator<>(
                neoStores,
                neoStores.getPropertyKeyTokenStore(),
                allocatorProvider.allocator(PROPERTY_KEY_TOKEN_NAME),
                contextFactory,
                memoryTracker);
    }

    public static TokenCreator directRelationshipTypeTokenCreator(
            NeoStores neoStores,
            DynamicAllocatorProvider allocatorProvider,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        return new DirectTokenCreator<>(
                neoStores,
                neoStores.getRelationshipTypeTokenStore(),
                allocatorProvider.allocator(RELATIONSHIP_TYPE_TOKEN_NAME),
                contextFactory,
                memoryTracker);
    }
}
