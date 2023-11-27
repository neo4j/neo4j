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

import static org.neo4j.internal.recordstorage.DirectTokenCreator.directLabelTokenCreator;
import static org.neo4j.internal.recordstorage.DirectTokenCreator.directPropertyKeyTokenCreator;
import static org.neo4j.internal.recordstorage.DirectTokenCreator.directRelationshipTypeTokenCreator;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

import java.util.List;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.TokenStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.ReadOnlyTokenCreator;
import org.neo4j.token.RegisteringCreatingTokenHolder;
import org.neo4j.token.TokenHolders;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;
import org.neo4j.token.api.TokensLoader;

/**
 * Utility methods for getting tokens out of token stores in various ways.
 */
public class StoreTokens {
    private StoreTokens() {}

    /**
     * Get a {@link TokensLoader} that loads tokens by reading the relevant token stores from the given {@link NeoStores}.
     * <p>
     * Note that this will ignore any tokens that cannot be read, for instance due to a store inconsistency, or if the store needs to be recovered.
     * If you would rather have an exception thrown, then you need to {@link TokenHolder#setInitialTokens(List) set the initial tokens} on each of the token
     * holders, using tokens read via the {@link TokenStore#getTokens(StoreCursors)} method,
     * instead of the {@link TokenStore#getAllReadableTokens(StoreCursors)} method.
     *
     * @param neoStores The {@link NeoStores} to read tokens from.
     */
    public static TokensLoader allReadableTokens(NeoStores neoStores) {
        return new TokensLoader() {
            @Override
            public List<NamedToken> getPropertyKeyTokens(StoreCursors storeCursors) {
                return neoStores.getPropertyKeyTokenStore().getAllReadableTokens(storeCursors);
            }

            @Override
            public List<NamedToken> getLabelTokens(StoreCursors storeCursors) {
                return neoStores.getLabelTokenStore().getAllReadableTokens(storeCursors);
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens(StoreCursors storeCursors) {
                return neoStores.getRelationshipTypeTokenStore().getAllReadableTokens(storeCursors);
            }
        };
    }

    /**
     * Get a {@link TokensLoader} that loads tokens by reading the relevant token stores from the given {@link NeoStores}.
     * <p>
     * This loader will throw exceptions if it encounters a token that cannot be read due to an inconsistency.
     *
     * @param neoStores The {@link NeoStores} to read tokens from.
     */
    public static TokensLoader allTokens(NeoStores neoStores) {
        return new TokensLoader() {
            @Override
            public List<NamedToken> getPropertyKeyTokens(StoreCursors storeCursors) {
                return neoStores.getPropertyKeyTokenStore().getTokens(storeCursors);
            }

            @Override
            public List<NamedToken> getLabelTokens(StoreCursors storeCursors) {
                return neoStores.getLabelTokenStore().getTokens(storeCursors);
            }

            @Override
            public List<NamedToken> getRelationshipTypeTokens(StoreCursors storeCursors) {
                return neoStores.getRelationshipTypeTokenStore().getTokens(storeCursors);
            }
        };
    }

    /**
     * Create read-only token holders initialised with the tokens from the given {@link NeoStores}.
     * <p>
     * Note that this call will ignore tokens that cannot be loaded due to inconsistencies, rather than throwing an exception.
     * The reason for this is that the read-only token holders are primarily used by tools, such as the consistency checker.
     *
     * @param neoStores The {@link NeoStores} from which to load the initial tokens.
     * @return TokenHolders that can be used for reading tokens, but cannot create new ones.
     */
    public static TokenHolders readOnlyTokenHolders(NeoStores neoStores, StoreCursors storeCursors) {
        TokenHolder propertyKeyTokens = createReadOnlyTokenHolder(TYPE_PROPERTY_KEY);
        TokenHolder labelTokens = createReadOnlyTokenHolder(TYPE_LABEL);
        TokenHolder relationshipTypeTokens = createReadOnlyTokenHolder(TYPE_RELATIONSHIP_TYPE);
        TokenHolders tokenHolders = new TokenHolders(propertyKeyTokens, labelTokens, relationshipTypeTokens);
        tokenHolders.setInitialTokens(allReadableTokens(neoStores), storeCursors);
        return tokenHolders;
    }

    /**
     * Create an empty read-only token holder of the given type.
     * @param tokenType one of {@link TokenHolder#TYPE_LABEL}, {@link TokenHolder#TYPE_RELATIONSHIP_TYPE}, or {@link TokenHolder#TYPE_PROPERTY_KEY}.
     * @return An empty read-only token holder.
     */
    public static TokenHolder createReadOnlyTokenHolder(String tokenType) {
        return new CreatingTokenHolder(ReadOnlyTokenCreator.READ_ONLY, tokenType);
    }

    public static TokenHolders directTokenHolders(
            NeoStores neoStores,
            DynamicAllocatorProvider allocatorProvider,
            CursorContextFactory contextFactory,
            MemoryTracker memoryTracker) {
        TokenHolders tokenHolders = new TokenHolders(
                new RegisteringCreatingTokenHolder(
                        directPropertyKeyTokenCreator(neoStores, allocatorProvider, contextFactory, memoryTracker),
                        TYPE_PROPERTY_KEY),
                new RegisteringCreatingTokenHolder(
                        directLabelTokenCreator(neoStores, allocatorProvider, contextFactory, memoryTracker),
                        TYPE_LABEL),
                new RegisteringCreatingTokenHolder(
                        directRelationshipTypeTokenCreator(neoStores, allocatorProvider, contextFactory, memoryTracker),
                        TYPE_RELATIONSHIP_TYPE));
        try (CursorContext cursorContext = contextFactory.create("load tokens");
                StoreCursors storeCursors = new CachedStoreCursors(neoStores, cursorContext)) {
            tokenHolders
                    .propertyKeyTokens()
                    .setInitialTokens(neoStores.getPropertyKeyTokenStore().getTokens(storeCursors));
            tokenHolders
                    .labelTokens()
                    .setInitialTokens(neoStores.getLabelTokenStore().getTokens(storeCursors));
            tokenHolders
                    .relationshipTypeTokens()
                    .setInitialTokens(neoStores.getRelationshipTypeTokenStore().getTokens(storeCursors));
        }
        return tokenHolders;
    }
}
