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

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.impl.store.id.IdType;

/**
 * Id type configuration provider for community edition.
 * Allow to reuse predefined set of id types.
 * @see IdType
 * @see IdTypeConfiguration
 */
public class CommunityIdTypeConfigurationProvider implements IdTypeConfigurationProvider
{

    private static final Set<IdType> TYPES_TO_ALLOW_REUSE =
            Collections.unmodifiableSet( EnumSet.of( IdType.PROPERTY, IdType.STRING_BLOCK,
                    IdType.ARRAY_BLOCK, IdType.NODE_LABELS ) );

    private final Map<IdType,IdTypeConfiguration> typeConfigurations = new EnumMap<>( IdType.class );

    @Override
    public IdTypeConfiguration getIdTypeConfiguration( IdType idType )
    {
        return typeConfigurations.computeIfAbsent( idType, this::createIdTypeConfiguration );
    }

    private IdTypeConfiguration createIdTypeConfiguration( IdType idType )
    {
        return new IdTypeConfiguration( getTypesToReuse().contains( idType ) );
    }

    protected Set<IdType> getTypesToReuse()
    {
        return TYPES_TO_ALLOW_REUSE;
    }
}
