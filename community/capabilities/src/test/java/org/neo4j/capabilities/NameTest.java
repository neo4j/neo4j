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
package org.neo4j.capabilities;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class NameTest {

    @Test
    void testConstruction() {
        var root = Name.of("");
        assertThat(root.fullName()).isEqualTo("");
        assertThat(root.toString()).isEqualTo("");

        var dbms = Name.of("dbms");
        assertThat(dbms.fullName()).isEqualTo("dbms");
        assertThat(dbms.toString()).isEqualTo("dbms");

        var dbmsInstance = Name.of("dbms.instance");
        assertThat(dbmsInstance.fullName()).isEqualTo("dbms.instance");
        assertThat(dbmsInstance.toString()).isEqualTo("dbms.instance");

        dbmsInstance = dbms.child("instance");
        assertThat(dbmsInstance.fullName()).isEqualTo("dbms.instance");
        assertThat(dbmsInstance.toString()).isEqualTo("dbms.instance");
    }

    @Test
    void testIsIn() {
        var root = Name.of("");
        var dbms = root.child("dbms");
        var dbmsInstance = dbms.child("instance");
        var dbmsInstanceKernel = dbmsInstance.child("kernel");

        assertThat(dbmsInstanceKernel.isIn(dbmsInstanceKernel)).isTrue();
        assertThat(dbmsInstanceKernel.isIn(dbmsInstance)).isTrue();
        assertThat(dbmsInstanceKernel.isIn(dbms)).isTrue();
        assertThat(dbmsInstanceKernel.isIn(root)).isTrue();

        assertThat(dbmsInstanceKernel.isIn("dbms.instance.kernel")).isTrue();
        assertThat(dbmsInstanceKernel.isIn("dbms.instance")).isTrue();
        assertThat(dbmsInstanceKernel.isIn("dbms")).isTrue();
        assertThat(dbmsInstanceKernel.isIn("")).isTrue();

        assertThat(dbmsInstance.isIn("dbms.cluster")).isFalse();
        assertThat(dbmsInstance.isIn("cluster")).isFalse();
        assertThat(dbmsInstance.isIn(dbmsInstanceKernel)).isFalse();
        assertThat(root.isIn(dbms)).isFalse();
    }

    @Test
    void testEquals() {
        assertThat(Name.of("").equals(Name.of(""))).isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").equals(Name.of("dbms.instance.kernel.version")))
                .isTrue();
        assertThat(Name.of("dbms.instance.kernel.version")
                        .equals(Name.of("dbms.instance.kernel").child("version")))
                .isTrue();

        assertThat(Name.of("dbms.instance").equals(Name.of("cluster.instance"))).isFalse();
        assertThat(Name.of("dbms").equals(Name.of(""))).isFalse();

        assertThat(Name.of("dbms").equals("dbms")).isFalse();
        assertThat(Name.of("dbms").equals(null)).isFalse();
    }

    @Test
    void testChildThrowsWhenInvalid() {
        var root = Name.of("");

        assertThatThrownBy(() -> root.child(""))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'' is not a valid name");
        assertThatThrownBy(() -> root.child("dbms.instance"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'dbms.instance' is not a valid name");
    }

    @Test
    void testThrowsOnInvalidName() {
        assertThatCode(() -> Name.of("")).doesNotThrowAnyException();
        assertThatCode(() -> Name.of("dbms")).doesNotThrowAnyException();
        assertThatCode(() -> Name.of("dbms.instance")).doesNotThrowAnyException();

        assertThatCode(() -> Name.of("my-dbms.instance"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'my-dbms.instance' is not a valid name.");
        assertThatCode(() -> Name.of("dbms.instance."))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'dbms.instance.' is not a valid name.");
        assertThatCode(() -> Name.of(".dbms.instance"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("'.dbms.instance' is not a valid name.");
    }

    @Test
    void testMatches() {
        assertThat(Name.of("dbms").matches("dbms")).isTrue();
        assertThat(Name.of("dbms.instance").matches("dbms.instance")).isTrue();
        assertThat(Name.of("dbms.instance").matches("dbms.instance2")).isFalse();
        assertThat(Name.of("dbms.instance").matches("dbms.*")).isTrue();
        assertThat(Name.of("dbms.instance").matches("dbms.inst*")).isTrue();
        assertThat(Name.of("dbms.instance").matches("dbms.instance*")).isTrue();
        assertThat(Name.of("dbms.instance").matches("dbms.**")).isTrue();
        assertThat(Name.of("dbms.instance").matches("dbms.**.version")).isFalse();
        assertThat(Name.of("dbms.instance").matches("*.instance")).isTrue();
        assertThat(Name.of("dbms.instance.version").matches("dbms.instance.*")).isTrue();
        assertThat(Name.of("dbms.instance.version").matches("dbms.instance.**")).isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").matches("dbms.instance.*.version"))
                .isTrue();
        assertThat(Name.of("dbms.instance.bolt.version").matches("dbms.instance.*.version"))
                .isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").matches("dbms.**")).isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").matches("dbms.**.version"))
                .isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").matches("dbms.**.allowed"))
                .isFalse();
        assertThat(Name.of("dbms.instance.kernel.version").matches("**.allowed"))
                .isFalse();
        assertThat(Name.of("dbms.instance.kernel.version").matches("**.instance.**"))
                .isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").matches("**instance**"))
                .isTrue();
        assertThat(Name.of("dbms.instance.kernel.version").matches("**instance.**"))
                .isTrue();
    }
}
