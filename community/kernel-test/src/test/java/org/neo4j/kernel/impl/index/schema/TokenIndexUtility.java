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
package org.neo4j.kernel.impl.index.schema;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.list.mutable.primitive.LongArrayList;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.test.RandomSupport;

public class TokenIndexUtility {
    static final long[] TOKENS = {1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L};

    /**
     * Compares the state of the tree with the expected values.
     * @param expected Mapping from entity id to expected entity tokens.
     */
    static void verifyUpdates(
            MutableLongObjectMap<long[]> expected,
            TokenScanLayout layout,
            Supplier<GBPTree<TokenScanKey, TokenScanValue>> treeSupplier,
            DefaultTokenIndexIdLayout idLayout)
            throws IOException {
        // Verify that everything in the tree is expected to exist.
        try (GBPTree<TokenScanKey, TokenScanValue> tree = treeSupplier.get();
                Seeker<TokenScanKey, TokenScanValue> scan = scan(tree, layout)) {
            while (scan.next()) {
                TokenScanKey key = scan.key();
                long bits = scan.value().bits;
                long entityIdBase = idLayout.firstIdOfRange(key.idRange);
                for (int i = 0; i < Long.SIZE; i++) {
                    long mask = 1L << i;
                    long posInBits = bits & mask;
                    if (posInBits != 0) {
                        long entity = entityIdBase + i;
                        long[] tokens = expected.remove(entity);
                        assertThat(tokens)
                                .withFailMessage(
                                        "Entity " + entity + " contained unexpected token " + key.tokenId + " in tree")
                                .contains(key.tokenId);

                        // Put back the rest of the tokens that we haven't verified yet
                        if (tokens.length != 1) {
                            expected.put(entity, ArrayUtils.removeElement(tokens, key.tokenId));
                        }
                    }
                }
            }
        }

        // Verify that nothing expected was missing from the tree
        expected.forEachKeyValue((entityId, tokenIds) -> assertThat(tokenIds)
                .withFailMessage("Tokens " + Arrays.toString(tokenIds) + " not found in tree for entity " + entityId)
                .isEmpty());
    }

    private static Seeker<TokenScanKey, TokenScanValue> scan(
            GBPTree<TokenScanKey, TokenScanValue> tree, TokenScanLayout layout) throws IOException {
        TokenScanKey lowest = layout.newKey();
        layout.initializeAsLowest(lowest);
        TokenScanKey highest = layout.newKey();
        layout.initializeAsHighest(highest);
        return tree.seek(lowest, highest, NULL_CONTEXT);
    }

    static List<TokenIndexEntryUpdate<?>> generateSomeRandomUpdates(
            MutableLongObjectMap<long[]> entityTokens, RandomSupport random) {
        long currentScanId = 0;
        List<TokenIndexEntryUpdate<?>> updates = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            generateRandomUpdate(currentScanId, entityTokens, updates, random);

            // Advance scan
            currentScanId++;
        }
        return updates;
    }

    static void generateRandomUpdate(
            long entityId,
            MutableLongObjectMap<long[]> trackingState,
            List<TokenIndexEntryUpdate<?>> updates,
            RandomSupport random) {
        long[] addTokens = generateRandomTokens(random);
        if (addTokens.length != 0) {
            TokenIndexEntryUpdate<?> update = IndexEntryUpdate.change(entityId, null, EMPTY_LONG_ARRAY, addTokens);
            updates.add(update);

            // Add update to tracking structure
            trackingState.put(entityId, Arrays.copyOf(addTokens, addTokens.length));
        }
    }

    /**
     * Generate array of random tokens.
     * Generated array is empty with a certain probability.
     * Generated array contains specific tokens with different probability to get varying distribution - some bitset
     * should be quite full, while others should be quite empty and more likely to become empty with later updates.
     */
    static long[] generateRandomTokens(RandomSupport random) {
        long[] allTokens = TokenIndexUtility.TOKENS;
        double[] allTokensRatio = new double[] {0.9, 0.8, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.01, 0.001};
        double emptyRatio = 0.1;

        if (random.nextDouble() < emptyRatio) {
            return EMPTY_LONG_ARRAY;
        } else {
            LongArrayList longArrayList = new LongArrayList();

            for (int i = 0; i < allTokens.length; i++) {
                if (random.nextDouble() < allTokensRatio[i]) {
                    longArrayList.add(allTokens[i]);
                }
            }
            return longArrayList.toArray();
        }
    }
}
