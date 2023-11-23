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
package org.neo4j.kernel.api.index;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_INT_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
import static org.neo4j.internal.schema.SchemaDescriptors.ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.IndexEntryUpdate;

class EntityTokenUpdatesTest {
    private static final long ENTITY_ID = 0;
    private static final int TOKEN_1_ID = 0;
    private static final int TOKEN_2_ID = 1;

    private static final int[] EMPTY = EMPTY_INT_ARRAY;

    @ParameterizedTest
    @EnumSource(Entity.class)
    void shouldNotGenerateUpdatesForEmptyEntityUpdates(Entity entity) {
        EntityUpdates updates = EntityUpdates.forEntity(ENTITY_ID, false).build();
        assertThat(updates.tokenUpdateForIndexKey(entity.getTokenIndex())).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Entity.class)
    void shouldNotGenerateUpdateForExistingToken(Entity entity) {
        EntityUpdates updates =
                EntityUpdates.forEntity(ENTITY_ID, false).withTokens(TOKEN_1_ID).build();
        assertThat(updates.tokenUpdateForIndexKey(entity.getTokenIndex())).isEmpty();
    }

    @ParameterizedTest
    @EnumSource(Entity.class)
    void shouldGenerateUpdateForAddedTokens(Entity entity) {
        EntityUpdates updates = EntityUpdates.forEntity(ENTITY_ID, false)
                .withTokens(EMPTY)
                .withTokensAfter(TOKEN_1_ID, TOKEN_2_ID)
                .build();
        assertThat(updates.tokenUpdateForIndexKey(entity.getTokenIndex()))
                .contains(IndexEntryUpdate.change(
                        ENTITY_ID, entity.getTokenIndex(), EMPTY, new int[] {TOKEN_1_ID, TOKEN_2_ID}));
    }

    @ParameterizedTest
    @EnumSource(Entity.class)
    void shouldGenerateUpdateForChangedTokens(Entity entity) {
        EntityUpdates updates = EntityUpdates.forEntity(ENTITY_ID, false)
                .withTokens(TOKEN_1_ID)
                .withTokensAfter(TOKEN_1_ID, TOKEN_2_ID)
                .build();
        assertThat(updates.tokenUpdateForIndexKey(entity.getTokenIndex()))
                .contains(IndexEntryUpdate.change(
                        ENTITY_ID, entity.getTokenIndex(), new int[] {TOKEN_1_ID}, new int[] {TOKEN_1_ID, TOKEN_2_ID}));
    }

    @ParameterizedTest
    @EnumSource(Entity.class)
    void shouldGenerateUpdateForRemovedTokens(Entity entity) {
        EntityUpdates updates = EntityUpdates.forEntity(ENTITY_ID, false)
                .withTokens(TOKEN_1_ID, TOKEN_2_ID)
                .withTokensAfter(EMPTY)
                .build();
        assertThat(updates.tokenUpdateForIndexKey(entity.getTokenIndex()))
                .contains(IndexEntryUpdate.change(
                        ENTITY_ID, entity.getTokenIndex(), new int[] {TOKEN_1_ID, TOKEN_2_ID}, EMPTY));
    }

    private enum Entity {
        NODE {
            @Override
            SchemaDescriptorSupplier getTokenIndex() {
                return () -> ANY_TOKEN_NODE_SCHEMA_DESCRIPTOR;
            }
        },
        RELATIONSHIP {
            @Override
            SchemaDescriptorSupplier getTokenIndex() {
                return () -> ANY_TOKEN_RELATIONSHIP_SCHEMA_DESCRIPTOR;
            }
        };

        abstract SchemaDescriptorSupplier getTokenIndex();
    }
}
