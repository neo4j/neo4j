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
package org.neo4j.server.startup;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.lang3.SystemUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BootloaderOsAbstractionTest {

    @TempDir
    private Path javaHome;

    @Test
    void shouldResolveJavaBinaryFromJavaHome() throws IOException {
        Path java =
                Files.createDirectory(javaHome.resolve("bin")).resolve(SystemUtils.IS_OS_WINDOWS ? "java.exe" : "java");
        Files.createFile(java);
        java.toFile().setExecutable(true);

        assertThat(BootloaderOsAbstraction.javaBinaryInJavaHome(javaHome.toString()))
                .isEqualTo(java);
    }

    @Test
    void shouldNotResolveJavaBinaryWithoutJavaHome() {
        assertThat(BootloaderOsAbstraction.javaBinaryInJavaHome("")).isNull();
    }

    @Test
    void shouldNotResolveJavaBinaryWhenJavaHomeHasNoJava() {
        assertThat(BootloaderOsAbstraction.javaBinaryInJavaHome(javaHome.toString()))
                .isNull();
    }
}
