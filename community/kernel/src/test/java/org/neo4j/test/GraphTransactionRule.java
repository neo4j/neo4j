/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test;

import org.junit.rules.ExternalResource;

import org.neo4j.graphdb.Transaction;

/**
 * JUnit @Rule for running a transaction for the duration of a test. Requires an EmbeddedDatabaseRule with
 * whose database the transaction will be executed.
 */
public class GraphTransactionRule
    extends ExternalResource
{
    private final DatabaseRule database;

    private Transaction tx;

    public GraphTransactionRule( DatabaseRule database )
    {
        this.database = database;
    }

    @Override
    protected void before()
        throws Throwable
    {
        begin();
    }

    @Override
    protected void after()
    {
        success();
    }

    public Transaction current()
    {
        return tx;
    }

    public Transaction begin()
    {
        tx = database.getGraphDatabaseService().beginTx();
        return tx;
    }

    public void success()
    {
        try
        {
            if (tx != null)
            {
                tx.success();
                tx.close();
            }
        }
        finally
        {
            tx = null;
        }
    }



    public void failure()
    {
        try
        {
            if (tx != null)
            {
                tx.failure();
                tx.close();
            }
        }
        finally
        {
            tx = null;
        }
    }
}
