/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
import static org.neo4j.server.ServerTestUtils.createTempDir;

import java.io.IOException;

import javax.management.MalformedObjectNameException;

import org.junit.Test;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.server.database.Database;

public class DatabasePrimitivesSampleableBaseTest
{

    @Test
    public void shouldSurviveDatabaseSwitch() throws MalformedObjectNameException, IOException {
        
        EmbeddedGraphDatabase db = new EmbeddedGraphDatabase( createTempDir().getAbsolutePath() );
        Database database = new Database(db);
        DatabasePrimitivesSampleableBase sampleable = new NodeIdsInUseSampleable( database ); 
        
        assertTrue("There should be a single node in use.", sampleable.getValue() == 1);
        
        try {
            database.graph.shutdown();
            sampleable.getValue();
            throw new RuntimeException("Expected UnableToSampleException to be thrown.");
        } catch(UnableToSampleException e) {
            // Skip
        }
        
        database.graph = new EmbeddedGraphDatabase( createTempDir().getAbsolutePath() );
        
        assertTrue("There should be a single node in use.", sampleable.getValue() == 1);
        
        database.shutdown();
        
    }
    
}
