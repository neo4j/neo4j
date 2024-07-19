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

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.server.security.SecureHasher;
import org.neo4j.server.security.SystemGraphCredential;
import org.neo4j.string.UTF8;

class UserSerializationTest {

    @Test
    void shouldSerializeAndDeserialize() throws Exception {
        // Given
        UserSerialization serialization = new UserSerialization();
        List<User> users = legacyUsers();

        // When
        byte[] serialized = serialization.serialize(users);

        // Then
        assertThat(serialization.deserializeRecords(serialized)).isEqualTo(users);
    }

    @Test
    void shouldSerializeAndDeserializeSystemGraphCredentialPassword() throws Exception {
        // Given
        UserSerialization serialization = new UserSerialization();
        List<User> users = users();

        // When
        byte[] serialized = serialization.serialize(users);

        // Then
        List<User> actual = serialization.deserializeRecords(serialized);
        assertThat(actual.size()).isEqualTo(users.size());
        for (int i = 0; i < actual.size(); i++) {
            // they should be in the same order so this is okay
            User actualUser = actual.get(i);
            User givenUser = users.get(i);
            assertThat(actualUser.name()).isEqualTo(givenUser.name());
            assertThat(actualUser.credential().value().serialize())
                    .isEqualTo(givenUser.credential().value().serialize());
        }
    }

    @Test
    void shouldMaskAndUnmaskSerializedSystemGraphCredential() {
        // Given
        List<User> users = users();

        for (User user : users) {
            String serialized = user.credential().value().serialize();

            // When
            String masked = SystemGraphCredential.maskSerialized(serialized);

            // Then
            assertThat(serialized).isEqualTo(SystemGraphCredential.serialize(masked.getBytes()));
        }
    }

    @Test
    void shouldMaskAndUnmaskSerializedCredentialsMissingIterations() {
        // Given
        List<User> users = legacyUsers();

        for (User user : users) {
            String serialized = user.credential().value().serialize();
            // Newer versions of the LegacyCredentials add the iteration count, but there might still exist old
            // serializations in the graph
            // that are missing this field, so we remove that field in this test to simulate that.
            String serializedWithoutIterations = serialized.substring(0, serialized.lastIndexOf(","));

            // When
            String masked = SystemGraphCredential.maskSerialized(serializedWithoutIterations);

            // Then (now all serialized credentials will include iteration count)
            assertThat(serialized).isEqualTo(SystemGraphCredential.serialize(masked.getBytes()));
        }
    }

    /**
     * This is a future-proofing test. If you come here because you've made changes to the serialization format,
     * this is your reminder to make sure to build this is in a backwards compatible way.
     */
    @Test
    void shouldReadV1SerializationFormat() throws Exception {
        // Given
        UserSerialization serialization = new UserSerialization();
        byte[] salt1 = {(byte) 0xa5, (byte) 0x43};
        byte[] hash1 = {(byte) 0xfe, (byte) 0x00, (byte) 0x56, (byte) 0xc3, (byte) 0x7e};
        byte[] salt2 = {(byte) 0x34, (byte) 0xa4};
        byte[] hash2 = {(byte) 0x0e, (byte) 0x1f, (byte) 0xff, (byte) 0xc2, (byte) 0x3e};

        // When
        List<User> deserialized = serialization.deserializeRecords(UTF8.encode(
                "Mike:SHA-256,FE0056C37E,A543:\n" + "Steve:SHA-256,FE0056C37E,A543:nice_guy,password_change_required\n"
                        + "Bob:SHA-256,0E1FFFC23E,34A4:password_change_required\n"));

        // Then
        assertThat(deserialized)
                .isEqualTo(asList(
                        new User("Mike", null, new LegacyCredential(salt1, hash1), false, false),
                        new User("Steve", null, new LegacyCredential(salt1, hash1), true, false),
                        new User("Bob", null, new LegacyCredential(salt2, hash2), true, false)));
    }

    private List<User> legacyUsers() {
        return asList(
                new User("Mike", null, LegacyCredential.forPassword("1234321"), false, false),
                new User("Steve", null, LegacyCredential.forPassword("1234321"), false, false),
                new User("steve.stevesson@WINDOMAIN", null, LegacyCredential.forPassword("1234321"), false, false),
                new User("Bob", null, LegacyCredential.forPassword("0987654"), false, false));
    }

    private List<User> users() {
        SecureHasher hasher = new SecureHasher();
        return asList(
                new User(
                        "Mike",
                        "id1",
                        SystemGraphCredential.createCredentialForPassword(UTF8.encode("1234321"), hasher),
                        false,
                        false),
                new User(
                        "Steve",
                        "id2",
                        SystemGraphCredential.createCredentialForPassword(UTF8.encode("1234321"), hasher),
                        false,
                        false),
                new User(
                        "steve.stevesson@WINDOMAIN",
                        "id3",
                        SystemGraphCredential.createCredentialForPassword(UTF8.encode("1234321"), hasher),
                        false,
                        false),
                new User(
                        "Bob",
                        null,
                        SystemGraphCredential.createCredentialForPassword(UTF8.encode("0987654"), hasher),
                        false,
                        false));
    }
}
