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
package org.neo4j.graphdb;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.enterprise.EnterpriseFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.monitoring.Monitors;

import static org.neo4j.kernel.GraphDatabaseDependencies.newDependencies;

public class EnterpriseGraphDatabase extends GraphDatabaseFacade
{

    public EnterpriseGraphDatabase( File storeDir, Map<String,String> params,
            Iterable<KernelExtensionFactory<?>> kernelExtensions,
            Monitors monitors )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class )
                .kernelExtensions( kernelExtensions ).monitors( monitors ) );
    }

    public EnterpriseGraphDatabase( File storeDir, Map<String,String> params,
            Iterable<KernelExtensionFactory<?>> kernelExtensions )
    {
        this( storeDir, params, newDependencies()
                .settingsClasses( GraphDatabaseSettings.class )
                .kernelExtensions( kernelExtensions ) );
    }

    public EnterpriseGraphDatabase( File storeDir, Map<String,String> params,
            GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        new EnterpriseFacadeFactory().newFacade( storeDir, params, dependencies, this );
    }
}