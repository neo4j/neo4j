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
package org.neo4j.test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;

public class BatchTransaction
{
    private static final int MAX_SIZE = 10000;

    public static BatchTransaction beginBatchTx( GraphDatabaseService db )
    {
        return new BatchTransaction( db );
    }
    
    private final GraphDatabaseService db;
    private Transaction tx;

    private BatchTransaction( GraphDatabaseService db )
    {
        this.db = db;
        beginTx();
    }

    private void beginTx()
    {
        this.tx = db.beginTx();
    }

    public void restart()
    {
        finish();
        beginTx();
    }
    
    public void finish()
    {
        tx.success();
        tx.close();
    }

    public int limit()
    {
        return MAX_SIZE;
    }
}
