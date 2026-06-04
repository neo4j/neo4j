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
package org.neo4j.kernel.impl.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.security.auth.LegacyCredential.INACCESSIBLE;

import org.junit.jupiter.api.Test;
import org.neo4j.server.security.auth.LegacyCredential;

class LegacyCredentialTest {
    @Test
    void testMatchesPassword() {
        LegacyCredential credential = LegacyCredential.forPassword("foo");
        assertThat(credential.matchesPassword("foo")).isTrue();
        assertThat(credential.matchesPassword("fooo")).isFalse();
        assertThat(credential.matchesPassword("fo")).isFalse();
        assertThat(credential.matchesPassword("bar")).isFalse();
    }

    @Test
    void testEquals() {
        LegacyCredential credential = LegacyCredential.forPassword("foo");
        LegacyCredential sameCredential = new LegacyCredential(credential.salt(), credential.passwordHash());
        assertThat(credential).isEqualTo(sameCredential);
    }

    @Test
    void testInaccessibleCredentials() {
        LegacyCredential credential = new LegacyCredential(INACCESSIBLE.salt(), INACCESSIBLE.passwordHash());

        // equals
        assertThat(credential).isEqualTo(INACCESSIBLE);
        assertThat(credential).isEqualTo(INACCESSIBLE);
        assertThat(INACCESSIBLE).isEqualTo(INACCESSIBLE);
        assertThat(LegacyCredential.forPassword("")).isNotEqualTo(INACCESSIBLE);
        assertThat(LegacyCredential.forPassword("")).isNotEqualTo(INACCESSIBLE);

        // matchesPassword
        assertThat(INACCESSIBLE.matchesPassword(new String(new byte[] {}))).isFalse();
        assertThat(INACCESSIBLE.matchesPassword("foo")).isFalse();
        assertThat(INACCESSIBLE.matchesPassword("")).isFalse();
    }
}
