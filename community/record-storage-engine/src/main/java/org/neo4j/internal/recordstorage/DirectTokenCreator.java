/*
 * Copyright (c) "Neo4j"
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

import static org.neo4j.kernel.impl.store.PropertyStore.encodeString;

import java.util.Collection;
import org.neo4j.exceptions.KernelException;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
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

    public DirectTokenCreator(
            NeoStores neoStores,
            TokenStore<R> store,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        this.neoStores = neoStores;
        this.store = store;
        this.contextFactory = contextFactory;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public int createToken(String name, boolean internal) throws KernelException {
        int keyId;
        try (CursorContext cursorContext = contextFactory.create("create token");
                StoreCursors storeCursors = new CachedStoreCursors(neoStores, cursorContext);
                PageCursor cursor = store.openPageCursorForWriting(0, cursorContext)) {
            keyId = (int) store.nextId(cursorContext);
            R record = store.newRecord();
            record.setId(keyId);
            record.setInUse(true);
            record.setInternal(internal);
            record.setCreated();
            Collection<DynamicRecord> keyRecords =
                    store.allocateNameRecords(encodeString(name), cursorContext, memoryTracker);
            record.setNameId((int) Iterables.first(keyRecords).getId());
            record.addNameRecords(keyRecords);
            store.updateRecord(record, cursor, cursorContext, storeCursors);
        }
        return keyId;
    }

    public static TokenCreator directLabelTokenCreator(
            NeoStores neoStores, CursorContextFactory contextFactory, MemoryTracker memoryTracker) {
        return new DirectTokenCreator<>(neoStores, neoStores.getLabelTokenStore(), contextFactory, memoryTracker);
    }

    public static TokenCreator directPropertyKeyTokenCreator(
            NeoStores neoStores, CursorContextFactory contextFactory, MemoryTracker memoryTracker) {
        return new DirectTokenCreator<>(neoStores, neoStores.getPropertyKeyTokenStore(), contextFactory, memoryTracker);
    }

    public static TokenCreator directRelationshipTypeTokenCreator(
            NeoStores neoStores, CursorContextFactory contextFactory, MemoryTracker memoryTracker) {
        return new DirectTokenCreator<>(
                neoStores, neoStores.getRelationshipTypeTokenStore(), contextFactory, memoryTracker);
    }
}
