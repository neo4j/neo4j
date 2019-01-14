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
package org.neo4j.kernel.impl.store.id.configuration;

import org.neo4j.kernel.impl.store.id.IdType;

/**
 * Edition based configuration provider for possible id types.
 * @see IdType
 * @see IdTypeConfiguration
 */
public interface IdTypeConfigurationProvider
{
    /**
     * Provides configuration object for requested id type.
     * @param idType id type
     * @return edition based id type configuration
     */
    IdTypeConfiguration getIdTypeConfiguration( IdType idType );
}
