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
package org.neo4j.kernel.impl.newapi;

import java.io.File;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class WriteTestSupport implements KernelAPIWriteTestSupport
{
    private GraphDatabaseService db;
    protected DatabaseManagementService managementService;

    @Override
    public void setup( File storeDir )
    {
        db = newDb( storeDir );
    }

    protected GraphDatabaseService newDb( File storeDir )
    {
        managementService = new TestDatabaseManagementServiceBuilder( storeDir ).impermanent().build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    @Override
    public void clearGraph()
    {
        GraphDatabaseServiceCleaner.cleanDatabaseContent( db );
        try ( Transaction tx = db.beginTx() )
        {
            PropertyContainer graphProperties = graphProperties();
            for ( String key : graphProperties.getPropertyKeys() )
            {
                graphProperties.removeProperty( key );
            }
            tx.success();
        }
    }

    @Override
    public PropertyContainer graphProperties()
    {
        return ((GraphDatabaseAPI) db)
                .getDependencyResolver()
                .resolveDependency( EmbeddedProxySPI.class )
                .newGraphPropertiesProxy();
    }

    @Override
    public Kernel kernelToTest()
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) this.db).getDependencyResolver();
        return resolver.resolveDependency( Kernel.class );
    }

    @Override
    public GraphDatabaseService graphBackdoor()
    {
        return db;
    }

    @Override
    public void tearDown()
    {
        managementService.shutdown();
        db = null;
    }
}
