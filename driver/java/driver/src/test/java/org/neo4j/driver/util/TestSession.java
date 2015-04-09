/**
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
package org.neo4j.driver.util;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.Map;

import org.neo4j.Neo4j;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.Transaction;
import org.neo4j.driver.Value;

/**
 * A little utility for integration testing, this provides tests with a session they can work with
 */
public class TestSession implements TestRule, Session
{
    private Session realSession;
    private Neo4jRunner runner;

    @Override
    public Statement apply( final Statement base, Description description )
    {
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    runner = Neo4jRunner.getOrCreateGlobalServer();
                    runner.clearData();

                    realSession = Neo4j.session( "neo4j+http://localhost:7687" );
                    base.evaluate();
                }
                finally
                {
                    if ( realSession != null )
                    {
                        realSession.close();
                    }
                }
            }
        };
    }

    @Override
    public void close()
    {
        throw new UnsupportedOperationException( "Disallowed on this test session" );
    }

    @Override
    public Transaction newTransaction()
    {
        return realSession.newTransaction();
    }

    @Override
    public Result run( String statement, Map<String,Value> parameters )
    {
        return realSession.run( statement, parameters );
    }

    @Override
    public Result run( String statement )
    {
        return realSession.run( statement );
    }
}
