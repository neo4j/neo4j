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
package org.neo4j.shell.build;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.neo4j.shell.log.Logger;

/**
 * This class provides access to build time variables
 */
public class Build {
    private static final Logger log = Logger.create();
    private static Properties props;

    /**
     * Reads the build generated properties file the first time it is called.
     *
     * @return build properties
     */
    private static Properties getProperties() {
        if (props == null) {
            props = new Properties();
            try (InputStream stream = Build.class.getClassLoader().getResourceAsStream("build.properties")) {
                if (stream == null) {
                    throw new IllegalStateException("Cannot read build.properties");
                } else {
                    props.load(stream);
                }
            } catch (IOException e) {
                log.error(e);
                System.err.println("Could not read build properties: " + e.getMessage());
            }
        }

        return props;
    }

    /**
     * @return the revision of the source code, or "dev" if no properties file could be read.
     */
    public static String version() {
        return getProperties().getProperty("version", "dev");
    }

    /**
     * @return the revision of the Neo4j Driver, or "dev" if no properties file could be read.
     */
    public static String driverVersion() {
        return getProperties().getProperty("driverVersion", "dev");
    }
}
