/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertTrue;

import java.io.IOException;

import javax.management.MalformedObjectNameException;

import org.junit.Test;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.server.rrd.sampler.DatabasePrimitivesSampleableBase;
import org.neo4j.server.rrd.sampler.NodeIdsInUseSampleable;
import org.neo4j.test.ImpermanentGraphDatabase;

public class DatabasePrimitivesSampleableBaseTest
{
    @Test
    public void sampleTest() throws MalformedObjectNameException, IOException
    {
        InternalAbstractGraphDatabase db = new ImpermanentGraphDatabase();
        
        DatabasePrimitivesSampleableBase sampleable = new NodeIdsInUseSampleable( db );

        assertTrue( "There should be a single node in use.", sampleable.getValue() == 1 );

        db.shutdown();

        /*
        this makes no sense using direct object-references instead of jmx
        try
        {
            database.graph.shutdown();
            sampleable.getValue();
            throw new RuntimeException( "Expected UnableToSampleException to be thrown." );
        }
        catch ( UnableToSampleException e )
        {
            // Skip
        }

        database.graph = new EmbeddedGraphDatabase( createTempDir().getAbsolutePath() );

        assertTrue( "There should be a single node in use.", sampleable.getValue() == 1 );
        */
    }
}
