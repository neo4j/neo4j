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
package org.neo4j.graphdb.config;

import java.util.Map;
import java.util.function.Consumer;

/**
 * @deprecated The settings API will be completely rewritten in 4.0
 */
@Deprecated
public interface SettingValidator
{
    /**
     * Validate one or several setting values, throwing on invalid values.
     *
     * @param settings available to be validated
     * @param warningConsumer a consumer for configuration warnings
     * @return the set of settings considered valid by this validator
     * @throws InvalidSettingException if invalid value detected
     */
    Map<String,String> validate( Map<String,String> settings, Consumer<String> warningConsumer )
            throws InvalidSettingException;
}

