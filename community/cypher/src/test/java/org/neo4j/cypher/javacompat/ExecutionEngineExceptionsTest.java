/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.javacompat;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cypher.CouldNotCreateConstraintException;
import org.neo4j.cypher.CouldNotDropIndexException;
import org.neo4j.cypher.SyntaxException;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ExecutionEngineExceptionsTest
{
    @Rule
    public DatabaseRule databaseRule = new ImpermanentDatabaseRule();

    @Test
    public void shouldThrowSensibleExceptionOnFailingToCreateConstraint() throws Exception
    {
        // given
        ExecutionEngine engine = new ExecutionEngine( databaseRule.getGraphDatabaseService() );
        engine.execute( "CREATE ( person1:Person { name: 'One' } ), ( person2:Person { name: 'One' } )" );

        // when
        try
        {
            engine.execute( "CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE" );
            fail();
        }
        // then
        catch ( CouldNotCreateConstraintException ex )
        {
            assertEquals( "Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE:\n" +
                    "Multiple nodes with label `Person` have property `name` = 'One':\n" +
                    "  existing node(1)\n" +
                    "  new node(2)", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowSensibleExceptionOnFailingToDropIndex() throws Exception
    {
        // given
        ExecutionEngine engine = new ExecutionEngine( databaseRule.getGraphDatabaseService() );

        // when
        try
        {
            engine.execute( "DROP INDEX ON :Person(name)" );
            fail();
        }
        // then
        catch ( CouldNotDropIndexException ex )
        {
            assertEquals( "Unable to drop INDEX ON :Person(name): No such INDEX ON :Person(name).", ex.getMessage() );
        }
    }

    @Test
    public void shouldThrowSyntaxException() throws Exception
    {
        // given
        ExecutionEngine engine = new ExecutionEngine( databaseRule.getGraphDatabaseService() );

        // when
        try
        {
            engine.execute( "not cypher" );
            fail();
        }
        // then
        catch ( SyntaxException ex )
        {
            // expected
        }
    }
}
