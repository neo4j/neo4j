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

import org.ops4j.pax.runner.platform.*;
import org.ops4j.pax.runner.platform.internal.PlatformImpl;
import org.ops4j.util.property.PropertiesPropertyResolver;

import java.util.*;

/**
 * A {@link org.ops4j.pax.runner.platform.Platform} which can be observed.
 */
public class ObservablePlatform extends PlatformImpl implements Platform {
    private FluentPlatformBuilder platformBuilder;

    /**
     * Creates a new platform.
     *
     * @param platformBuilder concrete platform builder; mandatory
     */
    public ObservablePlatform(FluentPlatformBuilder platformBuilder) {
        super(platformBuilder);
        this.platformBuilder = platformBuilder;
        setResolver(new PropertiesPropertyResolver(null));
    }


    public String getMainClassName() {
        return platformBuilder.getMainClassName();
    }

    public String[] getArguments() {
        return platformBuilder.getArguments(null);
    }

    public String[] getVMOptions() {
        return platformBuilder.getVMOptions(null);
    }

    public List<SystemFileReference> getSystemFiles() {
        return platformBuilder.getSystemFileReferences();
    }

    public Dictionary getConfiguration() {
        return platformBuilder.getConfiguration();
    }

    public void start() throws PlatformException {
        List<SystemFileReference> systemFiles = getSystemFiles();
        List<BundleReference> bundles = Collections.emptyList();
        Properties properties = null;
        Dictionary config = getConfiguration();
        JavaRunner runner = null;

        log(systemFiles);

        start(systemFiles, bundles, properties, config, runner);
    }

    private void log(List<SystemFileReference> systemFiles) {
        System.out.println("SystemFileReferences...");
        for (SystemFileReference systemFile : systemFiles) {
            System.out.println("\t" + systemFile.getName() + " from " + systemFile.getURL());
        }
    }

}
