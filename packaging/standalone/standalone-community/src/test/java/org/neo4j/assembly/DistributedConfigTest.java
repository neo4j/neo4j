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
package org.neo4j.assembly;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;

@TestDirectoryExtension
class DistributedConfigTest {
    private static final Pattern SETTING_MATCHER = Pattern.compile("^(\\w+(\\.\\w+)+)\\s*=(.*)$");

    @Inject
    TestDirectory testDirectory;

    @Test
    void allSettingsShouldBeValid() throws IOException {
        List<String> mentionedSettings = new ArrayList<>();
        Path of = Path.of("src", "main", "distribution", "text", "community", "conf", "neo4j.conf");
        List<String> lines = Files.readAllLines(of);
        Path newConfig = testDirectory.file("tmp.conf");
        boolean multiLine = false;
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(newConfig))) {
            for (String line : lines) {
                multiLine = processLine(mentionedSettings, writer, line, multiLine);
            }
        }

        // Throws on errors
        Config config = Config.newBuilder()
                .set(GraphDatabaseSettings.strict_config_validation, Boolean.TRUE)
                .fromFile(newConfig)
                .build();

        // Check the settings without values
        Map<String, Setting<Object>> availableSettings = config.getDeclaredSettings();
        for (String mentionedSetting : mentionedSettings) {
            assertTrue(availableSettings.containsKey(mentionedSetting));
        }
    }

    private static boolean processLine(
            List<String> mentionedSettings, PrintWriter writer, String line, boolean multiLine) {
        if (!line.startsWith("#")) {
            writer.println(line);
            return false;
        }
        // This is a comment
        String uncommented = line.substring(1).trim();

        Matcher matcher = SETTING_MATCHER.matcher(uncommented);
        if (matcher.matches()) {
            // This is a commented setting
            if (matcher.group(3).isEmpty()) {
                // And we have no default value, we have to probe the config later
                mentionedSettings.add(matcher.group(1));
            } else {
                // We have a fully working setting, write it out to the file.
                writer.println(uncommented);
            }
            return uncommented.endsWith("\\");
        } else if (multiLine && uncommented.endsWith("\\")) {
            writer.println(uncommented);
            return true;
        }
        return false;
    }
}
