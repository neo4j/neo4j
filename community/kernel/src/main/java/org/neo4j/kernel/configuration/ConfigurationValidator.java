/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

package org.neo4j.kernel.configuration;

import java.util.Collection;
import java.util.Map;
import javax.annotation.Nonnull;

import org.neo4j.graphdb.config.InvalidSettingException;
import org.neo4j.graphdb.config.SettingValidator;
import org.neo4j.logging.Log;

/**
 * Responsible for validating part of a configuration
 */
public interface ConfigurationValidator
{
    /**
     * Validate a config and return its subset of valid keys and values. Unknown settings should be discarded. Invalid
     * settings should cause an error.
     *
     * @param settingValidators which are available
     * @param rawConfig to validate
     * @param log for logging with
     * @param parsingFile true if reading config file, false otherwise
     * @return a Map of valid keys and values.
     * @throws InvalidSettingException in case of invalid values
     */
    @Nonnull
    Map<String,String> validate( @Nonnull Collection<SettingValidator> settingValidators,
            @Nonnull Map<String,String> rawConfig,
            @Nonnull Log log, boolean parsingFile ) throws InvalidSettingException;
}
