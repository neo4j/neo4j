/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.enterprise.id;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.IdTypeConfiguration;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

/**
 * Id type configuration provider for enterprise edition.
 * Allow to reuse predefined id types that are reused in community and in
 * addition to that allow additional id types to reuse be specified by
 * {@link EnterpriseEditionSettings#idTypesToReuse} setting.
 *
 * @see IdType
 * @see IdTypeConfiguration
 */
public class EnterpriseIdTypeConfigurationProvider extends CommunityIdTypeConfigurationProvider
{

    private final Set<IdType> typesToReuse;

    public EnterpriseIdTypeConfigurationProvider( Config config )
    {
        typesToReuse = configureReusableTypes( config );
    }

    @Override
    protected Set<IdType> getTypesToReuse()
    {
        return typesToReuse;
    }

    private EnumSet<IdType> configureReusableTypes( Config config )
    {
        EnumSet<IdType> types = EnumSet.copyOf( super.getTypesToReuse() );
        List<IdType> configuredTypes = config.get( EnterpriseEditionSettings.idTypesToReuse );
        types.addAll( configuredTypes );
        return types;
    }
}
