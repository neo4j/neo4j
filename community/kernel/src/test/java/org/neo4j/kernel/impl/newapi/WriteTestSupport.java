/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.impl.newapi;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.KernelAPIWriteTestSupport;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;

class WriteTestSupport implements KernelAPIWriteTestSupport
{
    private GraphDatabaseService db;

    @Override
    public void setup( File storeDir ) throws IOException
    {
        db = new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder( storeDir ).newGraphDatabase();
    }

    @Override
    public void clearGraph()
    {
        try ( Transaction tx = db.beginTx() )
        {

            for ( Relationship relationship : db.getAllRelationships() )
            {
                relationship.delete();
            }

            for ( Node node : db.getAllNodes() )
            {
                node.delete();
            }

            tx.success();
        }
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
