/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.release.it.std.exec;

import org.ops4j.pax.runner.platform.Configuration;
import org.ops4j.pax.runner.platform.PlatformBuilder;
import org.ops4j.pax.runner.platform.PlatformContext;
import org.ops4j.pax.runner.platform.PlatformException;

import java.io.IOException;
import java.io.InputStream;

/**
 * A pax-runner PlatformBuilder.
 */
public class Neo4jPlatformBuilder implements PlatformBuilder {

    /**
     * Provider name to be used in registration.
     */
    private static final String PROVIDER_NAME = "neo4j";

    /**
     * Provider version to be used in registration.
     */
    private static final String PROVIDER_VERSION = "0.1";

    /**
     * The directory name where the configuration will be stored.
     */
    private static final String CONFIG_DIRECTORY = "felix";
    /**
     * Configuration file name.
     */
    private static final String CONFIG_INI = "config.ini";
    /**
     * Caching directory.
     */
    private static final String CACHE_DIRECTORY = "cache";
    /**
     * Profile name to be used when console should be started.
     */
    private static final String CONSOLE_PROFILE = "tui";
    /**
     * Separator for properties (bundles)
     */
    private static final String SEPARATOR = " ";

    /**
     * Name of the main class from Felix.
     */
    private String mainClassName = "org.apache.felix.main.Main";

    /**
     * Args to be passed to the main class.
     */
    private String[] mainArgs;

    /**
     * Options to be passed to the jvm.
     */
    private String[] jvmOpts;

    public Neo4jPlatformBuilder(String mainClassName, String[] mainArgs, String[] jvmOpts) {
        this.mainClassName = mainClassName;
        this.mainArgs = mainArgs;
        this.jvmOpts = jvmOpts;
    }

    public void prepare(PlatformContext context) throws PlatformException {
        // TODO
    }

    public String getMainClassName() {
        return mainClassName;
    }

    public String[] getArguments(PlatformContext context) {
        return mainArgs;
    }

    public String[] getVMOptions(PlatformContext context) {
        return jvmOpts;
    }

    public InputStream getDefinition(Configuration configuration) throws IOException {
        return null;
    }

    public String getRequiredProfile(PlatformContext context) {
        return null;
    }

    public String getProviderName() {
        return PROVIDER_NAME;
    }

    public String getProviderVersion() {
        return PROVIDER_VERSION;
    }
}
