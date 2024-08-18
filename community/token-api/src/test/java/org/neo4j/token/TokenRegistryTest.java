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

import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.token.api.TokenConstants.NO_TOKEN;

import java.util.Collection;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.token.api.NamedToken;
import org.neo4j.token.api.NonUniqueTokenException;

class TokenRegistryTest {
    private static final String INBOUND2_TYPE = "inbound2";
    private static final String INBOUND1_TYPE = "inbound1";

    private TokenRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new TokenRegistry("testType");
    }

    @Test
    void puttingPublicTokenWithDuplicateNamedNotAllowed() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        registry.put(new NamedToken(INBOUND2_TYPE, 2));

        NamedToken token = new NamedToken(INBOUND1_TYPE, 3);
        NonUniqueTokenException exception = assertThrows(NonUniqueTokenException.class, () -> registry.put(token));
        assertThat(exception.getMessage()).contains(format("The testType %s is not unique", token));
    }

    @Test
    void puttingInternalTokenWithDuplicateNamedNotAllowed() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
        registry.put(new NamedToken(INBOUND2_TYPE, 2, true));

        NamedToken token = new NamedToken(INBOUND1_TYPE, 3, true);
        NonUniqueTokenException exception = assertThrows(NonUniqueTokenException.class, () -> registry.put(token));
        assertThat(exception.getMessage()).contains(format("The testType %s is not unique", token));
    }

    @Test
    void mustKeepOriginalPublicTokenWhenAddDuplicate() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        registry.put(new NamedToken(INBOUND2_TYPE, 2));

        assertThrows(NonUniqueTokenException.class, () -> registry.put(new NamedToken(INBOUND1_TYPE, 3)));

        assertEquals(1, registry.getId(INBOUND1_TYPE));
        assertEquals(2, registry.getId(INBOUND2_TYPE));
        assertNull(registry.getToken(3));
        assertNull(registry.getTokenInternal(3));
    }

    @Test
    void mustKeepOriginalInternalTokenWhenAddDuplicate() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
        registry.put(new NamedToken(INBOUND2_TYPE, 2, true));

        assertThrows(NonUniqueTokenException.class, () -> registry.put(new NamedToken(INBOUND1_TYPE, 3, true)));

        assertEquals(1, registry.getIdInternal(INBOUND1_TYPE));
        assertEquals(2, registry.getIdInternal(INBOUND2_TYPE));
        assertNull(registry.getTokenInternal(3));
        assertNull(registry.getToken(3));
    }

    @Test
    void putAllMustThrowOnDuplicateNameInTokensAdded() {
        assertThrows(
                NonUniqueTokenException.class,
                () -> registry.putAll(asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND1_TYPE, 2))));
    }

    @Test
    void putAllMustNotThrowWhenPublicAndInternalTokenHaveSameName() {
        registry.putAll(asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND1_TYPE, 2, true)));
        assertThat(registry.getId(INBOUND1_TYPE)).isEqualTo(1);
        assertThat(registry.getIdInternal(INBOUND1_TYPE)).isEqualTo(2);
        assertThat(registry.getToken(1)).isEqualTo(new NamedToken(INBOUND1_TYPE, 1));
        assertThat(registry.getTokenInternal(1)).isNull();
        assertThat(registry.getToken(2)).isNull();
        assertThat(registry.getTokenInternal(2)).isEqualTo(new NamedToken(INBOUND1_TYPE, 2, true));
    }

    @Test
    void putAllMustThrowOnDuplicateIdInTokensAdded() {
        assertThrows(
                NonUniqueTokenException.class,
                () -> registry.putAll(asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND2_TYPE, 1))));
    }

    @Test
    void putAllMustThrowOnDuplicateIdInTokensAddedEvenAcrossPublicAndInternalTokens() {
        assertThrows(
                NonUniqueTokenException.class,
                () -> registry.putAll(
                        asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND2_TYPE, 1, true))));
    }

    @Test
    void setInitialTokensMustThrowOnDuplicateNameInTokensAdded() {
        var first = new NamedToken(INBOUND1_TYPE, 1);
        var second = new NamedToken(INBOUND1_TYPE, 2);
        var e = assertThrows(NonUniqueTokenException.class, () -> registry.setInitialTokens(asList(first, second)));
        assertThat(e.getMessage()).contains(first.toString());
    }

    @Test
    void setInitialTokensMustNotThrowWhenPublicAndInternalTokenHasSameName() {
        registry.setInitialTokens(asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND1_TYPE, 2, true)));
    }

    @Test
    void setInitialTokensMustThrowOnDuplicateIdInTokensAdded() {
        assertThrows(
                NonUniqueTokenException.class,
                () -> registry.setInitialTokens(
                        asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND2_TYPE, 1))));
    }

    @Test
    void setInitialTokensMustThrowOnDuplicateIdInTokensAddedEvenAcrossPublicAndInternalTokens() {
        assertThrows(
                NonUniqueTokenException.class,
                () -> registry.setInitialTokens(
                        asList(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND2_TYPE, 1, true))));
    }

    @Test
    void putAllMustThrowOnDuplicateNameWithExistingToken() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        assertThrows(
                NonUniqueTokenException.class, () -> registry.putAll(singletonList(new NamedToken(INBOUND1_TYPE, 2))));
    }

    @Test
    void putAllMustNotThrowWhenInternalTokenDuplicatesNameOfPublicToken() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        registry.putAll(singletonList(new NamedToken(INBOUND1_TYPE, 2, true)));
    }

    @Test
    void putAllMustNotThrowWhenPublicTokenDuplicatesNameOfInternalToken() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
        registry.putAll(singletonList(new NamedToken(INBOUND1_TYPE, 2)));
    }

    @Test
    void putAllMustThrowOnDuplicateIdWithExistingToken() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        assertThrows(
                NonUniqueTokenException.class, () -> registry.putAll(singletonList(new NamedToken(INBOUND2_TYPE, 1))));
        assertThat(registry.getToken(1)).isEqualTo(new NamedToken(INBOUND1_TYPE, 1));
        assertThat(registry.getId(INBOUND1_TYPE)).isEqualTo(1);
        assertThat(registry.getId(INBOUND2_TYPE)).isEqualTo(NO_TOKEN);
    }

    @Test
    void putAllMustThrowOnDuplicateIdWithExistingTokenEvenAcrossPublicAndInternalTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        assertThrows(
                NonUniqueTokenException.class,
                () -> registry.putAll(singletonList(new NamedToken(INBOUND2_TYPE, 1, true))));
        assertThat(registry.getToken(1)).isEqualTo(new NamedToken(INBOUND1_TYPE, 1));
        assertThat(registry.getId(INBOUND1_TYPE)).isEqualTo(1);
        assertThat(registry.getId(INBOUND2_TYPE)).isEqualTo(NO_TOKEN);
        assertThat(registry.getIdInternal(INBOUND1_TYPE)).isEqualTo(NO_TOKEN);
        assertThat(registry.getIdInternal(INBOUND2_TYPE)).isEqualTo(NO_TOKEN);
    }

    @Test
    void putAllMustThrowOnDuplicateIdWithExistingTokenEvenAcrossInternalAndPublicTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
        assertThrows(
                NonUniqueTokenException.class, () -> registry.putAll(singletonList(new NamedToken(INBOUND2_TYPE, 1))));
        assertThat(registry.getTokenInternal(1)).isEqualTo(new NamedToken(INBOUND1_TYPE, 1, true));
        assertThat(registry.getToken(1)).isNull();
        assertThat(registry.getId(INBOUND1_TYPE)).isEqualTo(NO_TOKEN);
        assertThat(registry.getId(INBOUND2_TYPE)).isEqualTo(NO_TOKEN);
        assertThat(registry.getIdInternal(INBOUND1_TYPE)).isEqualTo(1);
        assertThat(registry.getIdInternal(INBOUND2_TYPE)).isEqualTo(NO_TOKEN);
    }

    @Test
    void setInitialTokensMustNotThrowOnDuplicateWithExistingToken() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        registry.setInitialTokens(singletonList(new NamedToken(INBOUND1_TYPE, 1)));
    }

    @Test
    void getIdMustNotFindInternalTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));

        assertThat(registry.getId(INBOUND1_TYPE)).isEqualTo(NO_TOKEN);
    }

    @Test
    void getIdInternalMustFindInternalTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));

        assertThat(registry.getIdInternal(INBOUND1_TYPE)).isEqualTo(1);
    }

    @Test
    void getIdInternalMustNotFindPublicTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));

        assertThat(registry.getIdInternal(INBOUND1_TYPE)).isEqualTo(NO_TOKEN);
    }

    @Test
    void getTokenMustNotFindInternalTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));

        assertThat(registry.getToken(1)).isNull();
    }

    @Test
    void getTokenInternalMustFindInternalTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));

        assertThat(registry.getTokenInternal(1)).isEqualTo(new NamedToken(INBOUND1_TYPE, 1, true));
    }

    @Test
    void getTokenInternalMustNotFindPublicTokens() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));

        assertThat(registry.getTokenInternal(1)).isNull();
    }

    @Test
    void allTokensMustIncludePublicTokensButNotIncludeInternalTokens() {
        registry.putAll(asList(
                new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND1_TYPE, 2, true),
                new NamedToken(INBOUND2_TYPE, 3), new NamedToken(INBOUND2_TYPE, 4, true)));

        Collection<NamedToken> tokens = registry.allTokens();
        assertThat(tokens).contains(new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND2_TYPE, 3));
    }

    @Test
    void allInternalTokensMustIncludeInternalTokensButNotIncludePublicTokens() {
        registry.putAll(asList(
                new NamedToken(INBOUND1_TYPE, 1), new NamedToken(INBOUND1_TYPE, 2, true),
                new NamedToken(INBOUND2_TYPE, 3), new NamedToken(INBOUND2_TYPE, 4, true)));

        Collection<NamedToken> tokens = registry.allInternalTokens();
        assertThat(tokens).contains(new NamedToken(INBOUND1_TYPE, 2, true), new NamedToken(INBOUND2_TYPE, 4, true));
    }

    @Test
    void sizeMustCountPublicTokensButNotInternalTokens() {
        registry.putAll(asList(
                new NamedToken(INBOUND1_TYPE, 1),
                new NamedToken(INBOUND2_TYPE, 3),
                new NamedToken(INBOUND2_TYPE, 4, true)));

        assertThat(registry.size()).isEqualTo(2);
    }

    @Test
    void sizeInternalMustCountInternalTokensButNotPublicTokens() {
        registry.putAll(asList(
                new NamedToken(INBOUND1_TYPE, 1),
                new NamedToken(INBOUND1_TYPE, 2, true),
                new NamedToken(INBOUND2_TYPE, 4, true)));

        assertThat(registry.sizeInternal()).isEqualTo(2);
    }

    @Test
    void puttingInternalTokenBySameNameAsPublicTokenMustNotConflict() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        // This mustn't throw:
        registry.put(new NamedToken(INBOUND1_TYPE, 2, true));
    }

    @Test
    void puttingPublicTokenBySameNameAsInternalTokenMustNotConflict() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
        // This mustn't throw:
        registry.put(new NamedToken(INBOUND1_TYPE, 2));
    }

    @Test
    void puttingTwoIdenticalPublicTokensShouldBeIgnored() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
        registry.put(new NamedToken(INBOUND1_TYPE, 1));
    }

    @Test
    void puttingTwoIdenticalInternalTokensShouldBeIgnored() {
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
        registry.put(new NamedToken(INBOUND1_TYPE, 1, true));
    }
}
