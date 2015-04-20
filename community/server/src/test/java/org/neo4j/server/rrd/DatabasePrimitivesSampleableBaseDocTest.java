/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.rrd;

import java.io.IOException;
import javax.management.MalformedObjectNameException;

import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.state.NeoStoreProvider;
import org.neo4j.server.rrd.sampler.DatabasePrimitivesSampleableBase;
import org.neo4j.server.rrd.sampler.NodeIdsInUseSampleable;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.junit.Assert.assertTrue;

public class DatabasePrimitivesSampleableBaseDocTest
{
    @Test
    public void sampleTest() throws MalformedObjectNameException, IOException
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();

        DatabasePrimitivesSampleableBase sampleable = new NodeIdsInUseSampleable( neoStore( db ) );

        assertTrue( "There should be no nodes in use.", sampleable.getValue() == 0 );

        db.shutdown();
    }

    @Test
    public void rrd_uses_temp_dir() throws Exception
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI)new TestGraphDatabaseFactory().newImpermanentDatabase();

        DatabasePrimitivesSampleableBase sampleable = new NodeIdsInUseSampleable( neoStore( db ) );

        assertTrue( "There should be no nodes in use.", sampleable.getValue() == 0 );

        db.shutdown();
    }

    private NeoStoreProvider neoStore( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( NeoStoreProvider.class );
    }
}
