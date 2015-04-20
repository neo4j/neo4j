/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.logging;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Logback converter that converts logger names to module names.
 * Module names are defined in modules.properties which must be in classpath
 */
public class ModuleMapper
{
    Map<String, String> modules;

    public ModuleMapper() throws IOException
    {
        Properties props = new Properties();
        modules = new HashMap<String, String>();

        // Load all module definitions
        Enumeration<URL> moduleDefinitions = getClass().getClassLoader().getResources("META-INF/modules.properties");

        while (moduleDefinitions.hasMoreElements())
        {
            URL url = moduleDefinitions.nextElement();
            props.load(new InputStreamReader(url.openStream()));
            for (Map.Entry<Object, Object> propEntry : props.entrySet())
            {
                String packages = modules.get(propEntry.getKey());
                if (packages == null)
                    packages = propEntry.getValue().toString();
                else
                    packages = packages+","+propEntry.getValue().toString();

                modules.put(propEntry.getKey().toString(), packages);
            }
            props.clear();
        }
    }

    public String map(String logger)
    {
        for (Map.Entry<String, String> moduleEntry : modules.entrySet())
        {
            String[] packages = moduleEntry.getValue().split(",");
            for (String aPackage : packages)
            {
                if (logger.startsWith(aPackage))
                    return moduleEntry.getKey();
            }
        }

        // No mapping found, return full logger name
        return logger;
    }
}
