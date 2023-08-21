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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.commandline.Util.isSameOrChildFile;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class UtilTest {
    @Test
    void correctlyIdentifySameOrChildFile() {
        Path home = Path.of(".").toAbsolutePath();
        assertTrue(isSameOrChildFile(home, home));
        assertTrue(isSameOrChildFile(home, home.resolve("a")));
        assertTrue(isSameOrChildFile(home.resolve("a/./b"), home.resolve("a/b")));
        assertTrue(isSameOrChildFile(home.resolve("a/b"), home.resolve("a/./b")));

        assertFalse(isSameOrChildFile(home.resolve("a"), home.resolve("b")));
    }
}
