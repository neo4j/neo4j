/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise;

import java.util.ArrayList;
import java.util.Collection;
import javax.annotation.Nonnull;

import org.neo4j.causalclustering.core.CausalClusterConfigurationValidator;
import org.neo4j.configuration.HaConfigurationValidator;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.CommunityBootstrapper;
import org.neo4j.server.NeoServer;

public class EnterpriseBootstrapper extends CommunityBootstrapper
{
    @Override
    protected NeoServer createNeoServer( Config configurator, GraphDatabaseDependencies dependencies,
            LogProvider userLogProvider )
    {
        return new EnterpriseNeoServer( configurator, dependencies, userLogProvider );
    }

    @Override
    @Nonnull
    protected Collection<ConfigurationValidator> configurationValidators()
    {
        ArrayList<ConfigurationValidator> validators = new ArrayList<>();
        validators.addAll( super.configurationValidators() );
        validators.add( new HaConfigurationValidator() );
        validators.add( new CausalClusterConfigurationValidator() );
        return validators;
    }
}
