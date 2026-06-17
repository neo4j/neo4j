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
package org.neo4j.commandline.dbms;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.commandline.Util.isSameOrChildFile;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class UtilTest {
    @Test
    void correctlyIdentifySameOrChildFile() {
        Path home = Path.of(".").toAbsolutePath();
        assertThat(isSameOrChildFile(home, home)).isTrue();
        assertThat(isSameOrChildFile(home, home.resolve("a"))).isTrue();
        assertThat(isSameOrChildFile(home.resolve("a/./b"), home.resolve("a/b")))
                .isTrue();
        assertThat(isSameOrChildFile(home.resolve("a/b"), home.resolve("a/./b")))
                .isTrue();

        assertThat(isSameOrChildFile(home.resolve("a"), home.resolve("b"))).isFalse();
    }
}
