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
package org.neo4j.kernel.diagnostics.providers;

import static java.lang.Boolean.getBoolean;
import static org.neo4j.configuration.GraphDatabaseSettings.neo4j_home;

import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.configuration.Config;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.NamedDiagnosticsProvider;
import org.neo4j.internal.helpers.Exceptions;

public class PackagingDiagnostics extends NamedDiagnosticsProvider {
    public static final String PACKAGING_INFO_FILENAME = "packaging_info";
    private static final boolean PRINT_PACKAGING_INFO_ERROR =
            getBoolean(PackagingDiagnostics.class.getName() + ".printException");
    private final Path home;

    PackagingDiagnostics(Config config) {
        super("Packaging");
        this.home = config.get(neo4j_home);
    }

    @Override
    public void dump(DiagnosticsLogger logger) {
        Path packagingInfoPath = home.resolve(PACKAGING_INFO_FILENAME);

        try {
            for (String line : Files.readAllLines(packagingInfoPath)) {
                if (line.startsWith("Version:")) {
                    continue;
                }
                logger.log(line);
            }
        } catch (Exception e) {
            logger.log(PACKAGING_INFO_FILENAME + " is not available.");
            if (PRINT_PACKAGING_INFO_ERROR) {
                logger.log("Exception occurred while reading " + PACKAGING_INFO_FILENAME + " file:"
                        + Exceptions.stringify(e));
            }
        }
    }
}
