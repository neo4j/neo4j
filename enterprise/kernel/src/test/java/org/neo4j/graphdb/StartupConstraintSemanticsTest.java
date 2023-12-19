/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.graphdb;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class StartupConstraintSemanticsTest
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

    @Test
    public void shouldNotAllowOpeningADatabaseWithPECInCommunityEdition()
    {
        assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( "CREATE CONSTRAINT ON (n:Draconian) ASSERT exists(n.required)",
                StandardConstraintSemantics.ERROR_MESSAGE_EXISTS );
    }

    @Test
    public void shouldNotAllowOpeningADatabaseWithNodeKeyInCommunityEdition()
    {
        assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( "CREATE CONSTRAINT ON (n:Draconian) ASSERT (n.required) IS NODE KEY",
                StandardConstraintSemantics.ERROR_MESSAGE_NODE_KEY );
    }

    @Test
    public void shouldAllowOpeningADatabaseWithUniqueConstraintInCommunityEdition()
    {
        assertThatCommunityCanStartOnNormalConstraint( "CREATE CONSTRAINT ON (n:Draconian) ASSERT (n.required) IS UNIQUE" );
    }

    private void assertThatCommunityCanStartOnNormalConstraint( String constraintCreationQuery )
    {
        // given
        GraphDatabaseService graphDb = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( dir.graphDbDir() );
        try
        {
            graphDb.execute( constraintCreationQuery );
        }
        finally
        {
            graphDb.shutdown();
        }
        graphDb = null;

        // when
        try
        {
            graphDb = new TestGraphDatabaseFactory().newEmbeddedDatabase( dir.graphDbDir() );
            // Should not get exception
        }
        finally
        {
            if ( graphDb != null )
            {
                graphDb.shutdown();
            }
        }
    }

    private void assertThatCommunityCannotStartOnEnterpriseOnlyConstraint( String constraintCreationQuery, String errorMessage )
    {
        // given
        GraphDatabaseService graphDb = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( dir.graphDbDir() );
        try
        {
            graphDb.execute( constraintCreationQuery );
        }
        finally
        {
            graphDb.shutdown();
        }
        graphDb = null;

        // when
        try
        {
            graphDb = new TestGraphDatabaseFactory().newEmbeddedDatabase( dir.graphDbDir() );
            fail( "should have failed to start!" );
        }
        // then
        catch ( Exception e )
        {
            Throwable error = Exceptions.rootCause( e );
            assertThat( error, instanceOf( IllegalStateException.class ) );
            assertEquals( errorMessage, error.getMessage() );
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
