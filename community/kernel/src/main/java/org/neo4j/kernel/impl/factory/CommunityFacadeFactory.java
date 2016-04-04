/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.factory;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;

import static org.neo4j.helpers.collection.Iterables.append;
import static org.neo4j.helpers.collection.Iterables.asList;
import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

/**
 * This facade creates instances of the Community edition of Neo4j.
 */
@Service.Implementation( GraphDatabaseFacadeFactory.class )
public class CommunityFacadeFactory extends GraphDatabaseFacadeFactory
{
    @Override
    public GraphDatabaseFacade newFacade( File storeDir, Map<String,String> params, Dependencies dependencies,
            GraphDatabaseFacade graphDatabaseFacade )
    {
        return super.newFacade( storeDir, params, newDependencies( dependencies ).settingsClasses(
                asList( append( GraphDatabaseSettings.class, dependencies.settingsClasses() ) ) ),
                graphDatabaseFacade );
    }

    protected EditionModule createEdition( PlatformModule platformModule )
    {
        return new CommunityEditionModule( platformModule );
    }

    @Override
    public DatabaseInfo databaseInfo()
    {
        return new DatabaseInfo( Edition.community, OperationalMode.single );
    }
}
