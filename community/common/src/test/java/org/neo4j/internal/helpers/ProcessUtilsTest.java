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
package org.neo4j.internal.helpers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.internal.helpers.ProcessUtils.start;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProcessUtilsTest {
    private static final String HELLO_WORLD = "Hello World";

    public static void main(String[] args) {
        System.out.println(HELLO_WORLD);
    }

    @Test
    void mustFindWorkingJavaExecutableAndClassPath() throws Exception {
        List<String> command = new ArrayList<>();
        command.add(ProcessUtils.getJavaExecutable().toString());
        command.add("-cp");
        command.add(ProcessUtils.getClassPath());
        command.add(getClass().getName());

        ProcessBuilder builder = new ProcessBuilder(command);
        Process process = builder.start();

        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = in.readLine();

        assertThat(process.waitFor()).isZero();
        assertThat(line).isEqualTo(HELLO_WORLD);
    }

    @Test
    void startJavaProcessUsingProcessUtil() throws IOException, InterruptedException {
        var process = start(pb -> {}, getClass().getName());

        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = in.readLine();

        assertThat(process.waitFor()).isZero();
        assertThat(line).isEqualTo(HELLO_WORLD);
    }
}
