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
package org.neo4j.server;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import org.neo4j.configuration.Config;
import picocli.CommandLine;

public class CommandLineArgs {
    @CommandLine.Option(names = "--home-dir", description = "path to NEO4J_HOME")
    Path homeDir;

    @CommandLine.Option(
            names = "--config-dir",
            description = "path to a directory that contains a neo4j.conf file",
            converter = ConfigFileConverter.class)
    Path configFile;

    @CommandLine.Option(names = "--expand-commands", description = "allow execution of commands from the config")
    boolean expandCommands;

    @CommandLine.Option(
            names = "--console-mode",
            description = "whether the bootstrapper should act as a console or daemon")
    boolean consoleMode;

    @CommandLine.Option(
            names = "-c",
            mapFallbackValue = "true",
            description = "override config values with -c key=value")
    Map<String, String> configOverrides = Collections.emptyMap();

    private CommandLineArgs() {}

    private static final class ConfigFileConverter implements CommandLine.ITypeConverter<Path> {
        @Override
        public Path convert(String value) {
            return value != null ? Path.of(value, Config.DEFAULT_CONFIG_FILE_NAME) : null;
        }
    }

    public static CommandLineArgs parse(String... args) {
        CommandLineArgs commandLineArgs = new CommandLineArgs();
        new CommandLine(commandLineArgs).parseArgs(args);
        return commandLineArgs;
    }
}
