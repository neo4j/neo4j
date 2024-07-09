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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.input.Group;
import org.neo4j.test.Race;

class GroupsTest {
    @Test
    void shouldHandleConcurrentGetOrCreate() throws Throwable {
        // GIVEN
        Groups groups = new Groups();
        Race race = new Race();
        String name = "MyGroup";
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            race.addContestant(() -> {
                Group group = groups.getOrCreate(name);
                assertEquals(0, group.id());
            });
        }

        // WHEN
        race.go();

        // THEN
        Group otherGroup = groups.getOrCreate("MyOtherGroup");
        assertThat(otherGroup.id()).isEqualTo(1);
    }

    @Test
    void shouldSupportMixedGroupModeInGetOrCreate() {
        // given
        Groups groups = new Groups();
        var globalGroup = groups.getOrCreate(null);

        // when
        var otherGroup = groups.getOrCreate("Something");

        // then
        assertThat(otherGroup).isNotEqualTo(globalGroup);
    }

    @Test
    void shouldSupportMixedGroupModeInGetOrCreate2() {
        // given
        Groups groups = new Groups();
        var otherGroup = groups.getOrCreate("Something");

        // when
        var globalGroup = groups.getOrCreate(null);

        // then
        assertThat(globalGroup).isNotEqualTo(otherGroup);
    }

    @Test
    void shouldGetCreatedGroup() {
        // given
        Groups groups = new Groups();
        String name = "Something";
        Group createdGroup = groups.getOrCreate(name);

        // when
        Group gottenGroup = groups.get(name);

        // then
        assertThat(gottenGroup).isEqualTo(createdGroup);
    }

    @Test
    void shouldGetGlobalGroup() {
        // given
        Groups groups = new Groups();
        groups.getOrCreate(null);

        // when
        Group group = groups.get(null);

        // then
        assertThat(group.name()).isNull();
        assertThat(group.descriptiveName()).isEqualTo("global id space");
    }

    @Test
    void shouldFailOnGettingNonExistentGroup() {
        // given
        Groups groups = new Groups();

        // when
        assertThrows(HeaderException.class, () -> groups.get("Something"));
    }
}
