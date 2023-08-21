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
package org.neo4j.configuration;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.annotations.service.Service;

/**
 * A group of settings that can represented in multiple instances.
 * In comparison with {@link SettingsDeclaration} where each setting only exists in a single instance
 */
@Service
@PublicApi
public interface GroupSetting {
    /**
     * The name is unique to one instance of a group
     *
     * @return the name of this group
     */
    String name();

    /**
     * The prefix is the same for all the settings in this group
     *
     * @return the prefix for this group
     */
    String getPrefix();
}
