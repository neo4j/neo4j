/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration;

import java.util.Map;

import org.neo4j.annotations.service.Service;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.logging.Log;

/**
 * Implementations are responsible for migrating deprecated {@link Setting}s.
 */
@Service
public interface SettingMigrator
{
    /**
     * This method is called before the String representations of Settings and values are parsed.
     * Deprecated values should be removed from the values set
     * Replacing values should be added to the values or defaultValues set, depending on desired migration
     * @param values The map uses the name of settings as keys, and textual representation of values, as values
     * @param defaultValues The map uses the name of settings as keys, and textual representation of default values, as values
     * @param log The log to relay warnings about the use of deprecated Settings
     */
    void migrate( Map<String,String> values, Map<String,String> defaultValues, Log log );
}
