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
package org.neo4j.cli;

import static picocli.CommandLine.Help.Visibility.NEVER;

import java.nio.file.Path;
import org.neo4j.configuration.GraphDatabaseSettings;
import picocli.CommandLine.Option;

public class PathOptions {
    private static final String DEFAULT_DATA_PATH = "<config: " + GraphDatabaseSettings.DATA_DIRECTORY_SETTING_NAME
            + ">/" + GraphDatabaseSettings.DEFAULT_DATABASES_ROOT_DIR_NAME;
    private static final String DEFAULT_TXN_PATH =
            "<config: " + GraphDatabaseSettings.TRANSACTION_LOGS_ROOT_PATH_SETTING_NAME + ">";

    public static class SourceOptions {
        @Option(
                names = "--from-path-data",
                arity = "1",
                showDefaultValue = NEVER, // manually handled
                paramLabel = "<path>",
                description = "Path to the databases directory, containing the database directory to source from.%n  "
                        + "Default: " + DEFAULT_DATA_PATH,
                required = true)
        private Path dataPath;

        public Path dataPath() {
            return dataPath.toAbsolutePath().normalize();
        }

        @Option(
                names = "--from-path-txn",
                arity = "1",
                showDefaultValue = NEVER, // manually handled
                paramLabel = "<path>",
                description =
                        "Path to the transactions directory, containing the transaction directory for the database "
                                + "to source from.%n  Default: " + DEFAULT_TXN_PATH,
                required = true)
        private Path txnPath;

        public Path txnPath() {
            return txnPath.toAbsolutePath().normalize();
        }
    }

    public static class TargetOptions {
        @Option(
                names = "--to-path-data",
                arity = "1",
                showDefaultValue = NEVER, // manually handled
                paramLabel = "<path>",
                description = "Path to the databases directory, containing the database directory to target from.%n  "
                        + "Default: " + DEFAULT_DATA_PATH,
                required = true)
        private Path dataPath;

        public Path dataPath() {
            return dataPath.toAbsolutePath().normalize();
        }

        @Option(
                names = "--to-path-txn",
                arity = "1",
                showDefaultValue = NEVER, // manually handled
                paramLabel = "<path>",
                description =
                        "Path to the transactions directory containing the transaction directory for the database to "
                                + "target from.%n  Default: " + DEFAULT_TXN_PATH,
                required = true)
        private Path txnPath;

        public Path txnPath() {
            return txnPath.toAbsolutePath().normalize();
        }
    }
}
