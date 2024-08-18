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
package org.neo4j.token;

import static org.neo4j.function.Predicates.ALWAYS_TRUE_INT;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.function.IntPredicate;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.neo4j.exceptions.KernelException;

/**
 * Keeps a registry of tokens using {@link TokenRegistry}.
 * When asked for a token that isn't in the registry, delegates to a {@link TokenCreator} to create the token.
 * The storing of a created token in the registry is the responsibility of the creator
 * (in the regular transaction based flow it happens on transaction application).
 */
public class CreatingTokenHolder extends AbstractTokenHolderBase {
    private final TokenCreator tokenCreator;

    public CreatingTokenHolder(TokenCreator tokenCreator, String tokenType) {
        this(new TokenRegistry(tokenType), tokenCreator);
    }

    public CreatingTokenHolder(TokenCreatorFactory tokenCreator, String tokenType) {
        super(new TokenRegistry(tokenType));
        this.tokenCreator = tokenCreator.create(tokenRegistry);
    }

    public CreatingTokenHolder(TokenRegistry registry, TokenCreator tokenCreator) {
        super(registry);
        this.tokenCreator = tokenCreator;
    }

    /**
     * Create and put new token in cache.
     *
     * @param name token name
     * @return newly created token id
     */
    @Override
    protected synchronized int createToken(String name, boolean internal) throws KernelException {
        int id = internal ? tokenRegistry.getIdInternal(name) : tokenRegistry.getId(name);
        if (id != NO_TOKEN) {
            return id;
        }

        id = tokenCreator.createToken(name, internal);
        return id;
    }

    @Override
    public void getOrCreateIds(String[] names, int[] ids) throws KernelException {
        innerBatchGetOrCreate(names, ids, false);
    }

    @Override
    public void getOrCreateInternalIds(String[] names, int[] ids) throws KernelException {
        innerBatchGetOrCreate(names, ids, true);
    }

    private void innerBatchGetOrCreate(String[] names, int[] ids, boolean internal) throws KernelException {
        assertSameArrayLength(names, ids);
        // Assume all tokens exist and try to resolve them. Break out on the first missing token.
        boolean hasUnresolvedTokens = resolveIds(names, ids, internal, ALWAYS_TRUE_INT);

        if (hasUnresolvedTokens) {
            createMissingTokens(names, ids, internal);
        }
    }

    private static void assertSameArrayLength(String[] names, int[] ids) {
        if (names.length != ids.length) {
            throw new IllegalArgumentException("Name and id arrays must have the same length.");
        }
    }

    private synchronized void createMissingTokens(String[] names, int[] ids, boolean internal) throws KernelException {
        // We redo the resolving under the lock, to make sure that these ids are really missing, and won't be
        // created concurrently with us.
        MutableIntSet unresolvedIndexes = new IntHashSet();
        resolveIds(names, ids, internal, i -> !unresolvedIndexes.add(i));
        if (!unresolvedIndexes.isEmpty()) {
            // We still have unresolved ids to create.
            createUnresolvedTokens(unresolvedIndexes, names, ids, internal);
        }
    }

    private void createUnresolvedTokens(IntSet unresolvedIndexes, String[] names, int[] ids, boolean internal)
            throws KernelException {
        // First, we need to filter out all of the tokens that are already resolved, so we only create tokens for
        // indexes that are in the unresolvedIndexes set.
        // However, we also need to deal with duplicate token names. For any token index we decide needs to have a
        // token created, we will add a mapping from the token name, to the ids-index into which the token id will
        // be written. This is the 'createdTokens' map. It maps token names to indexes into the 'ids' array.
        // If we find that the 'created'Tokens' map already has an entry for a given name, then that name is a
        // duplicate, and we will need to "remap" it later, by reading the token id from the correct index in the
        // 'ids' array, and storing it at the indexes of the duplicates. This is what the 'remappingIndexes' map is
        // for. This is a map from 'a' to 'b', where both 'a' and 'b' are indexes into the 'ids' array, and where
        // the corresponding name for 'a' is a duplicate of the name for 'b', and where we have already decided
        // that we will create a token id for index 'b'. After the token ids have been created, we go through the
        // 'remappingIndexes' map, and for every '(a,b)' entry, we store the token id created for 'b' and 'ids'
        // index 'a'.
        ObjectIntHashMap<String> createdTokens = new ObjectIntHashMap<>();
        IntIntHashMap remappingIndexes = new IntIntHashMap();
        IntPredicate tokenCreateFilter = index -> {
            boolean needsCreate = unresolvedIndexes.contains(index);
            if (needsCreate) {
                // The name at this index is unresolved.
                String name = names[index];
                int creatingIndex = createdTokens.getIfAbsentPut(name, index);
                if (creatingIndex != index) {
                    // This entry has a duplicate name, so we need to remap this entry instead of creating a token
                    // for it.
                    remappingIndexes.put(index, creatingIndex);
                    needsCreate = false;
                }
            }
            return needsCreate;
        };

        // Create tokens for all the indexes that we don't filter out.
        tokenCreator.createTokens(names, ids, internal, tokenCreateFilter);

        // Remap duplicate tokens to the token id we created for the first instance of any duplicate token name.
        if (remappingIndexes.notEmpty()) {
            remappingIndexes.forEachKeyValue((index, creatingIndex) -> ids[index] = ids[creatingIndex]);
        }
    }
}
