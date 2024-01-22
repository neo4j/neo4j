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
package org.neo4j.server.security.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.security.auth.SecurityTestUtils.credentialFor;

import org.junit.jupiter.api.Test;
import org.neo4j.cypher.internal.security.SystemGraphCredential;
import org.neo4j.kernel.impl.security.User;

class UserTest {
    @Test
    @SuppressWarnings("TruthSelfEquals")
    void shouldBuildImmutableUser() {
        SystemGraphCredential abc = credentialFor("123abc");
        SystemGraphCredential fruit = credentialFor("fruit");
        User u1 = new User.Builder("Steve", abc).build();
        User u2 = new User.Builder("Steve", fruit)
                .withRequiredPasswordChange(true)
                .withFlag("nice_guy")
                .build();

        assertThat(u1).isEqualTo(u1);
        assertThat(u1).isNotEqualTo(u2);

        User u1AsU2 = u1.augment()
                .withCredentials(fruit)
                .withRequiredPasswordChange(true)
                .withFlag("nice_guy")
                .build();
        assertThat(u1).isNotEqualTo(u1AsU2);
        assertThat(u2).isEqualTo(u1AsU2);

        User u2AsU1 = u2.augment()
                .withCredentials(abc)
                .withRequiredPasswordChange(false)
                .withoutFlag("nice_guy")
                .build();
        assertThat(u2).isNotEqualTo(u2AsU1);
        assertThat(u1).isEqualTo(u2AsU1);

        assertThat(u1).isNotEqualTo(u2);
    }

    @Test
    @SuppressWarnings("TruthSelfEquals")
    void shouldBuildUserWithId() {
        SystemGraphCredential abc = credentialFor("123abc");
        User u1 = new User.Builder("Alice", abc).withId("id1").build();
        User u2 = new User.Builder("Alice", abc).withId("id2").build();
        User u3 = new User.Builder("Alice", abc).build();

        assertThat(u1).isEqualTo(u1);
        assertThat(u1).isNotEqualTo(u2);
        assertThat(u1).isNotEqualTo(u3);
        assertThat(u3).isNotEqualTo(u1);
    }
}
