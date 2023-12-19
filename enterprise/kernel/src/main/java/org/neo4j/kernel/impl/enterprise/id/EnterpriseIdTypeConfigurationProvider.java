/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.kernel.impl.enterprise.id;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.configuration.CommunityIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.store.id.configuration.IdTypeConfiguration;

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
