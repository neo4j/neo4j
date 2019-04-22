/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb.factory;

import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;

/**
 * Builder for {@link DatabaseManagementService}s that allows for setting and loading
 * configuration.
 */
public interface DatabaseManagementServiceInternalBuilder
{
    /**
     * Set a database setting to a particular value.
     *
     * @param setting Database setting to set
     * @param value New value of the setting
     * @return the builder
     */
    DatabaseManagementServiceInternalBuilder setConfig( Setting<?> setting, String value );

    /**
     * Set database settings from provided config. All previously configured overlapped config options will be overwritten.
     * @param config provided config
     * @return the builder
     */
    DatabaseManagementServiceInternalBuilder setConfig( Config config );

    /**
     * Set an unvalidated configuration option.
     *
     * @param name Name of the setting
     * @param value New value of the setting
     * @return the builder
     * @deprecated Use setConfig with explicit {@link Setting} instead.
     */
    @Deprecated
    DatabaseManagementServiceInternalBuilder setConfig( String name, String value );

    /**
     * Set a map of configuration settings into the builder. Overwrites any existing values.
     *
     * @param config Map of configuration settings
     * @return the builder
     * @deprecated Use setConfig with explicit {@link Setting} instead
     */
    @Deprecated
    DatabaseManagementServiceInternalBuilder setConfig( Map<String,String> config );

    /**
     * Load a Properties file from a given file, and add the settings to
     * the builder.
     *
     * @param fileName Filename of properties file to use
     * @return the builder
     * @throws IllegalArgumentException if the builder was unable to load from the given filename
     */
     DatabaseManagementServiceInternalBuilder loadPropertiesFromFile( String fileName )
            throws IllegalArgumentException;

     DatabaseManagementService newDatabaseManagementService();
}
