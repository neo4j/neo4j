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
package org.neo4j.kernel.impl.enterprise;

import org.neo4j.kernel.IdTypeConfigurationProvider;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.enterprise.id.EnterpriseIdTypeConfigurationProvider;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.impl.factory.StatementLocksFactorySelector;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.logging.LogService;

/**
 * This implementation of {@link EditionModule} creates the implementations of services
 * that are specific to the Enterprise edition, without HA
 */
public class EnterpriseEditionModule extends CommunityEditionModule
{
    public EnterpriseEditionModule( PlatformModule platformModule )
    {
        super( platformModule );
    }

    @Override
    protected IdTypeConfigurationProvider createIdTypeConfigurationProvider( Config config )
    {
        return new EnterpriseIdTypeConfigurationProvider( config );
    }

    @Override
    protected ConstraintSemantics createSchemaRuleVerifier()
    {
        return new EnterpriseConstraintSemantics();
    }

    @Override
    protected StatementLocksFactory createStatementLocksFactory( Locks locks, Config config, LogService logService )
    {
        return new StatementLocksFactorySelector( locks, config, logService ).select();
    }
}
