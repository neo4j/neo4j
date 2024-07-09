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
package org.neo4j.internal.batchimport.input;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.batchimport.api.InputIterable;
import org.neo4j.batchimport.api.ReadBehaviour;
import org.neo4j.batchimport.api.input.Collector;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.batchimport.api.input.IdType;
import org.neo4j.batchimport.api.input.Input;
import org.neo4j.batchimport.api.input.InputChunk;
import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.batchimport.api.input.ReadableGroups;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokenNotFoundException;

public class LenientStoreInput implements Input {
    private final PropertyStore propertyStore;
    private final NodeStore nodeStore;
    private final RelationshipStore relationshipStore;
    private final NeoStores neoStores;
    private final TokenHolders tokenHolders;
    private final boolean compactNodeIdSpace;
    private final CursorContextFactory contextFactory;
    private final ReadBehaviour readBehaviour;
    private final Groups groups = new Groups();
    private final Group inputGroup = groups.getOrCreate(null);

    public LenientStoreInput(
            NeoStores neoStores,
            TokenHolders tokenHolders,
            boolean compactNodeIdSpace,
            CursorContextFactory contextFactory,
            ReadBehaviour readBehaviour) {
        this.propertyStore = neoStores.getPropertyStore();
        this.nodeStore = neoStores.getNodeStore();
        this.relationshipStore = neoStores.getRelationshipStore();
        this.neoStores = neoStores;
        this.tokenHolders = tokenHolders;
        this.compactNodeIdSpace = compactNodeIdSpace;
        this.contextFactory = contextFactory;
        this.readBehaviour = readBehaviour;
    }

    @Override
    public InputIterable nodes(Collector badCollector) {
        return () -> new LenientInputChunkIterator(nodeStore) {
            @Override
            public InputChunk newChunk() {
                return new LenientNodeReader(
                        readBehaviour,
                        nodeStore,
                        propertyStore,
                        tokenHolders,
                        contextFactory,
                        new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT),
                        compactNodeIdSpace,
                        inputGroup);
            }
        };
    }

    @Override
    public InputIterable relationships(Collector badCollector) {
        return () -> new LenientInputChunkIterator(relationshipStore) {
            @Override
            public InputChunk newChunk() {
                return new LenientRelationshipReader(
                        readBehaviour,
                        relationshipStore,
                        propertyStore,
                        tokenHolders,
                        contextFactory,
                        new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT),
                        inputGroup);
            }
        };
    }

    @Override
    public IdType idType() {
        return IdType.INTEGER;
    }

    @Override
    public ReadableGroups groups() {
        return groups;
    }

    @Override
    public Estimates calculateEstimates(PropertySizeCalculator valueSizeCalculator) throws IOException {
        long propertyStoreSize = storeSize(propertyStore) / 2
                + storeSize(propertyStore.getStringStore()) / 2
                + storeSize(propertyStore.getArrayStore()) / 2;
        return Input.knownEstimates(
                nodeStore.getIdGenerator().getHighId(),
                relationshipStore.getIdGenerator().getHighId(),
                propertyStore.getIdGenerator().getHighId(),
                propertyStore.getIdGenerator().getHighId(),
                propertyStoreSize / 2,
                propertyStoreSize / 2,
                tokenHolders.labelTokens().size());
    }

    @Override
    public void close() {
        neoStores.close();
    }

    private static long storeSize(CommonAbstractStore<? extends AbstractBaseRecord, ? extends StoreHeader> store) {
        try {
            return store.getStoreSize();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static NamedToken getTokenByIdSafe(TokenHolder tokenHolder, int id) {
        try {
            return tokenHolder.getTokenById(id);
        } catch (TokenNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
