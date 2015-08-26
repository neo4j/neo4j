/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.fail;
import static org.neo4j.test.TargetDirectory.testDirForTest;

public class StartupConstraintSemanticsTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir = testDirForTest( StartupConstraintSemanticsTest.class );

    @Test
    public void shouldNotAllowOpeningADatabaseWithPECInCommunityEdition() throws Exception
    {
        // given
        GraphDatabaseService graphDb = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( dir.graphDbDir() );
        try
        {
            graphDb.execute( "CREATE CONSTRAINT ON (n:Draconian) ASSERT exists(n.required)" );
        }
        finally
        {
            graphDb.shutdown();
        }
        graphDb = null;

        // when
        try
        {
            graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( dir.graphDbDir() );
            fail( "should have failed to start!" );
        }
        // then
        catch ( Exception e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( graphDb != null )
            {
                graphDb.shutdown();
            }
        }
    }
}
