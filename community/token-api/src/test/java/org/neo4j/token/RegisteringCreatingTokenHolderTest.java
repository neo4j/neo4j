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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntPredicate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.exceptions.KernelException;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.TokenHolder;

class RegisteringCreatingTokenHolderTest {
    private TokenCreator creator;
    private TokenHolder holder;

    @BeforeEach
    void setUp() {
        creator = mock(TokenCreator.class);
        holder = new RegisteringCreatingTokenHolder(creator, "Dummy");
    }

    @Test
    void mustKnownItsTokenType() {
        assertEquals("Dummy", holder.getTokenType());
    }

    @Test
    void mustCreateAndCacheNewTokens() throws Exception {
        when(creator.createToken("token", false)).thenReturn(42);
        assertThat(holder.getOrCreateId("token")).isEqualTo(42);
        assertThat(holder.getOrCreateId("token")).isEqualTo(42);
        // Verify implies that the call only happens once.
        verify(creator).createToken("token", false);
        verifyNoMoreInteractions(creator);
    }

    @Test
    void mustBatchCreateAndCacheNewTokens() throws Exception {
        mockAssignNewTokenIdsInBatch(new AtomicInteger(42));
        String[] names = {"token"};
        int[] ids = new int[1];
        holder.getOrCreateIds(names, ids);
        assertThat(ids[0]).isEqualTo(42);
        holder.getOrCreateIds(names, ids);
        assertThat(ids[0]).isEqualTo(42);
        // Verify implies that the call only happens once.
        verify(creator).createTokens(any(String[].class), any(int[].class), eq(false), any(IntPredicate.class));
        verifyNoMoreInteractions(creator);
    }

    @Test
    void mustBatchCreateAndCacheNewInternalTokens() throws Exception {
        mockAssignNewTokenIdsInBatch(new AtomicInteger(42));
        String[] names = {"token"};
        int[] ids = new int[1];
        holder.getOrCreateInternalIds(names, ids);
        assertThat(ids[0]).isEqualTo(42);
        holder.getOrCreateInternalIds(names, ids);
        assertThat(ids[0]).isEqualTo(42);
        // Verify implies that the call only happens once.
        verify(creator).createTokens(any(String[].class), any(int[].class), eq(true), any(IntPredicate.class));
        verifyNoMoreInteractions(creator);
    }

    @Test
    void batchTokenGetMustReturnWhetherThereWereUnresolvedTokens() {
        holder.setInitialTokens(asList(token("a", 1), token("b", 2)));
        String[] names;
        int[] ids;

        names = new String[] {"a", "X", "b"};
        ids = new int[] {-1, -1, -1};
        assertTrue(holder.getIdsByNames(names, ids));
        assertThat(ids[0]).isEqualTo(1);
        assertThat(ids[1]).isEqualTo(-1);
        assertThat(ids[2]).isEqualTo(2);

        names = new String[] {"a", "b"};
        ids = new int[] {-1, -1};
        assertFalse(holder.getIdsByNames(names, ids));
        assertThat(ids[0]).isEqualTo(1);
        assertThat(ids[1]).isEqualTo(2);
    }

    @Test
    void batchTokenCreateMustIgnoreExistingTokens() throws Exception {
        initialTokensABC();

        AtomicInteger nextId = new AtomicInteger(42);
        mockAssignNewTokenIdsInBatch(nextId);

        String[] names = new String[] {"b", "X", "a", "Y", "c"};
        int[] ids = new int[names.length];
        holder.getOrCreateIds(names, ids);
        assertThat(ids.length).isEqualTo(5);
        assertThat(ids[0]).isEqualTo(2);
        assertThat(ids[1]).isIn(42, 43);
        assertThat(ids[2]).isEqualTo(1);
        assertThat(ids[3]).isIn(42, 43);
        assertThat(ids[4]).isEqualTo(3);
        assertThat(nextId.get()).isEqualTo(44);

        // And these should not throw.
        holder.getTokenById(42);
        holder.getTokenById(43);
    }

    @Test
    void batchTokenCreateInternalMustIgnoreExistingTokens() throws Exception {
        initialInternalTokensABC();

        AtomicInteger nextId = new AtomicInteger(42);
        mockAssignNewTokenIdsInBatch(nextId);

        String[] names = new String[] {"b", "X", "a", "Y", "c"};
        int[] ids = new int[names.length];
        holder.getOrCreateInternalIds(names, ids);
        assertThat(ids.length).isEqualTo(5);
        assertThat(ids[0]).isEqualTo(2);
        assertThat(ids[1]).isIn(42, 43);
        assertThat(ids[2]).isEqualTo(1);
        assertThat(ids[3]).isIn(42, 43);
        assertThat(ids[4]).isEqualTo(3);
        assertThat(nextId.get()).isEqualTo(44);

        // And these should not throw.
        holder.getInternalTokenById(42);
        holder.getInternalTokenById(43);
    }

    @Test
    void batchTokenCreateMustNotConfusePublicAndInternalTokens() throws KernelException {
        mockAssignNewTokenIdsInBatch(new AtomicInteger(10));

        int[] ids = new int[2];
        holder.getOrCreateIds(new String[] {"a", "b"}, ids);
        assertThat(ids[0]).isEqualTo(10);
        assertThat(ids[1]).isEqualTo(11);

        holder.getOrCreateInternalIds(new String[] {"b", "c"}, ids);
        assertThat(ids[0]).isEqualTo(12);
        assertThat(ids[1]).isEqualTo(13);

        holder.getOrCreateIds(new String[] {"b", "c"}, ids);
        assertThat(ids[0]).isEqualTo(11);
        assertThat(ids[1]).isEqualTo(14);

        holder.getOrCreateInternalIds(new String[] {"c", "d"}, ids);
        assertThat(ids[0]).isEqualTo(13);
        assertThat(ids[1]).isEqualTo(15);
    }

    private void mockAssignNewTokenIdsInBatch(AtomicInteger nextId) throws KernelException {
        doAnswer(inv -> {
                    int[] ids = inv.getArgument(1);
                    IntPredicate filter = inv.getArgument(3);
                    for (int i = 0; i < ids.length; i++) {
                        if (filter.test(i)) {
                            ids[i] = nextId.getAndIncrement();
                        }
                    }
                    return null;
                })
                .when(creator)
                .createTokens(any(String[].class), any(int[].class), any(boolean.class), any(IntPredicate.class));
    }

    private void initialTokensABC() throws KernelException {
        holder.setInitialTokens(asList(token("a", 1), token("b", 2)));

        when(creator.createToken("c", false)).thenReturn(3);
        assertThat(holder.getOrCreateId("c")).isEqualTo(3);
    }

    private void initialInternalTokensABC() {
        holder.setInitialTokens(asList(token("a", 1, true), token("b", 2, true), token("c", 3, true)));
    }

    @Test
    void batchTokenCreateMustDeduplicateTokenCreates() throws Exception {
        initialTokensABC();

        AtomicInteger nextId = new AtomicInteger(42);
        mockAssignNewTokenIdsInBatch(nextId);

        // NOTE: the existing 'b', and the missing 'X', tokens are in here twice:
        String[] names = new String[] {"b", "b", "X", "a", "X", "c"};
        int[] ids = new int[names.length];
        holder.getOrCreateIds(names, ids);

        assertThat(ids.length).isEqualTo(6);
        assertThat(ids[0]).isEqualTo(2);
        assertThat(ids[1]).isEqualTo(2);
        assertThat(ids[2]).isEqualTo(42);
        assertThat(ids[3]).isEqualTo(1);
        assertThat(ids[4]).isEqualTo(42);
        assertThat(ids[5]).isEqualTo(3);
        assertThat(nextId.get()).isEqualTo(43);

        // And this should not throw.
        holder.getTokenById(42);
    }

    @Test
    void batchTokenCreateMustThrowOnArraysOfDifferentLengths() {
        assertThrows(IllegalArgumentException.class, () -> holder.getOrCreateIds(new String[3], new int[2]));
    }

    @Test
    void shouldClearTokensAsPartOfInitialTokenLoading() {
        // GIVEN
        holder.setInitialTokens(asList(token("one", 1), token("two", 2)));
        assertTokens(holder.getAllTokens(), token("one", 1), token("two", 2));

        // WHEN
        holder.setInitialTokens(asList(token("two", 2), token("three", 3), token("four", 4)));

        // THEN
        assertTokens(holder.getAllTokens(), token("two", 2), token("three", 3), token("four", 4));
    }

    private static void assertTokens(Iterable<NamedToken> allTokens, NamedToken... expectedTokens) {
        Map<String, NamedToken> existing = new HashMap<>();
        for (NamedToken token : allTokens) {
            existing.put(token.name(), token);
        }
        Map<String, NamedToken> expected = new HashMap<>();
        for (NamedToken token : expectedTokens) {
            expected.put(token.name(), token);
        }
        assertEquals(expected, existing);
    }

    private static NamedToken token(String name, int id) {
        return token(name, id, false);
    }

    private static NamedToken token(String name, int id, boolean internal) {
        return new NamedToken(name, id, internal);
    }
}
