/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel;

import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

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

    private final Map<IdType,IdTypeConfiguration> typeConfigurations = new EnumMap<>(IdType.class);

    @Override
    public IdTypeConfiguration getIdTypeConfiguration( IdType idType )
    {
        IdTypeConfiguration typeConfiguration = typeConfigurations.get( idType );
        if ( typeConfiguration == null )
        {
            typeConfiguration = new IdTypeConfiguration( getTypesToReuse().contains( idType ) );
            typeConfigurations.put( idType, typeConfiguration );
        }
        return typeConfiguration;
    }

    protected Set<IdType> getTypesToReuse()
    {
        return TYPES_TO_ALLOW_REUSE;
    }
}
