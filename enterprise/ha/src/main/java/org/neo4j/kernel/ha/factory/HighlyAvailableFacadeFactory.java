/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.Edition;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.factory.PlatformModule;

/**
 * This facade creates instances of the Enterprise edition of Neo4j.
 */
@Service.Implementation( GraphDatabaseFacadeFactory.class )
public class HighlyAvailableFacadeFactory extends GraphDatabaseFacadeFactory
{
    @Override
    protected EditionModule createEdition( PlatformModule platformModule )
    {
        return new HighlyAvailableEditionModule(platformModule);
    }

    @Override
    public int selectionPriority()
    {
        return 1;
    }

    @Override
    public DatabaseInfo databaseInfo()
    {
        return new DatabaseInfo( Edition.enterprise, OperationalMode.ha );
    }
}
