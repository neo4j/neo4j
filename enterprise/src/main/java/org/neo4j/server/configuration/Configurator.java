/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server.configuration;

import java.io.File;
import java.io.FilenameFilter;
import java.util.HashMap;

import org.apache.commons.configuration.*;

public class Configurator {

    private File configDir = new File("etc");

    private HashMap<String, Configuration> config = new HashMap<String, Configuration>();

    public Configurator(File ... configDirs) {
        for (File configDir : configDirs) {
            loadConfigFrom( configDir );
        }
    }

    public Configuration getConfigurationFor(String componentName) {
        Configuration configuration = config.get(componentName);
        return configuration == null ? new SystemConfiguration() : configuration;
    }

    /**
     * Looks for files of the form <componentName>.<ext>
     *
     * @param configDir
     */
    private void loadConfigFrom(File configDir) {
        FilenameFilter filenameFilter = new FilenameFilter() {
            public boolean accept( File dir, String name )
            {
                return name.indexOf( "." ) > 0;
            }
        };
        for (File configFile : configDir.listFiles( filenameFilter )) {
            String filename = configFile.getName();
            String componentName = filename.substring( filename.lastIndexOf('.') + 1 );
            CompositeConfiguration componentConfig = new CompositeConfiguration();

            try
            {
                componentConfig.addConfiguration( new PropertiesConfiguration(filename) );
            } catch ( ConfigurationException e )
            {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
            config.put(componentName, componentConfig);

        }
    }

}
