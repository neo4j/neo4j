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
package org.neo4j.kernel.ha.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

/**
 * This facade creates instances of the Enterprise edition of Neo4j.
 */
public class HighlyAvailableFacadeFactory extends GraphDatabaseFacadeFactory
{
    @Override
    public GraphDatabaseFacade newFacade( File storeDir, Map<String, String> params, Dependencies dependencies,
            GraphDatabaseFacade graphDatabaseFacade )
    {
        params.put( Configuration.editionName.name(), "Enterprise");
        return super.newFacade( storeDir, params, dependencies, graphDatabaseFacade, OperationalMode.ha );
    }

    @Override
    protected EditionModule createEdition( PlatformModule platformModule )
    {
        return new HighlyAvailableEditionModule(platformModule);
    }
}
