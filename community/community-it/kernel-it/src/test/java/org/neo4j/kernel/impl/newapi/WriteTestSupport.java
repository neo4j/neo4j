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

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.KernelAPIWriteTestSupport;
import org.neo4j.kernel.impl.core.EmbeddedProxySPI;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.TestGraphDatabaseFactory;

class WriteTestSupport implements KernelAPIWriteTestSupport
{
    private GraphDatabaseService db;

    @Override
    public void setup( File storeDir )
    {
        db = newDb( storeDir );
    }

    protected GraphDatabaseService newDb( File storeDir )
    {
        return new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder( storeDir ).newGraphDatabase();
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
        db.shutdown();
        db = null;
    }
}
