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
package org.neo4j.messages;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * validate exception messages to prevent accidental changes
 */
class MessageUtilTest {
    @Test
    void createNodeDenied() {
        assertThat(MessageUtil.createNodeWithLabelsDenied("label", "db", "userDesc"))
                .isEqualTo("Create node with labels 'label' on database 'db' is not allowed for userDesc.");
    }

    @Test
    void withUser() {
        assertThat(MessageUtil.withUser("user", "mode")).isEqualTo("user 'user' with mode");
    }

    @Test
    void overridenMode() {
        assertThat(MessageUtil.overriddenMode("origin", "wrapping")).isEqualTo("origin overridden by wrapping");
    }

    @Test
    void restrictedMode() {
        assertThat(MessageUtil.restrictedMode("origin", "wrapping")).isEqualTo("origin restricted to wrapping");
    }

    @Test
    void authDisabled() {
        assertThat(MessageUtil.authDisabled("mode")).isEqualTo("AUTH_DISABLED with mode");
    }

    @Test
    void StandardMode() {
        assertThat(MessageUtil.standardMode(new HashSet<>())).isEqualTo("no roles");
        assertThat(MessageUtil.standardMode(Set.of("role1"))).isEqualTo("roles [role1]");
        assertThat(MessageUtil.standardMode(Set.of("role1", "role2"))).isEqualTo("roles [role1, role2]");
    }

    // alias

    @Test
    void alterToLocalAlias() {
        assertThat(MessageUtil.alterToLocalAlias("name"))
                .isEqualTo(
                        "Failed to alter the specified database alias 'name': alter a local alias to a remote alias is not supported.");
    }

    @Test
    void failedToFindEncryptionKeyInKeystore() {
        assertThat(MessageUtil.failedToFindEncryptionKeyInKeystore("name"))
                .isEqualTo(
                        "Failed to read the symmetric key from the configured keystore. The key 'name' was not found in the given keystore file.");
    }

    @Test
    void failedToReadRemoteAliasEncryptionKey() {
        assertThat(MessageUtil.failedToReadEncryptionKey("setting1", "setting2"))
                .isEqualTo(
                        "Failed to read the symmetric key from the configured keystore. Please verify the keystore configurations: setting1, setting2.");
    }

    @Test
    void failedToEncryptPassword() {
        assertThat(MessageUtil.failedToEncryptPassword()).isEqualTo("Failed to encrypt remote user password.");
    }

    @Test
    void failedToDecryptPassword() {
        assertThat(MessageUtil.failedToDecryptPassword()).isEqualTo("Failed to decrypt remote user password.");
    }

    @Test
    void invalidScheme() {
        assertThat(MessageUtil.invalidScheme("url", Arrays.asList("scheme1", "scheme2")))
                .isEqualTo(
                        "The provided url 'url' has an invalid scheme. Please use one of the following schemes: scheme1, scheme2.");
    }

    @Test
    void insecureScheme() {
        assertThat(MessageUtil.insecureScheme("url", Arrays.asList("scheme1", "scheme2")))
                .isEqualTo(
                        "The provided url 'url' is not a secure scheme. Please use one of the following schemes: scheme1, scheme2.");
    }
}
