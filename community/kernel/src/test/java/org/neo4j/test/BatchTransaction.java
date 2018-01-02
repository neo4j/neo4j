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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.progress.ProgressListener;

public class BatchTransaction implements AutoCloseable
{
    private static final int DEFAULT_INTERMEDIARY_SIZE = 10000;

    public static BatchTransaction beginBatchTx( GraphDatabaseService db )
    {
        return new BatchTransaction( db );
    }

    private final GraphDatabaseService db;
    private Transaction tx;
    private int txSize;
    private int total;
    private int intermediarySize = DEFAULT_INTERMEDIARY_SIZE;
    private ProgressListener progressListener = ProgressListener.NONE;

    private BatchTransaction( GraphDatabaseService db )
    {
        this.db = db;
        beginTx();
    }

    private void beginTx()
    {
        this.tx = db.beginTx();
    }

    public GraphDatabaseService getDb()
    {
        return db;
    }

    public boolean increment()
    {
        return increment( 1 );
    }

    public boolean increment( int count )
    {
        txSize += count;
        total += count;
        progressListener.add( count );
        if ( txSize >= intermediarySize )
        {
            txSize = 0;
            intermediaryCommit();
            return true;
        }
        return false;
    }

    public void intermediaryCommit()
    {
        closeTx();
        beginTx();
    }

    private void closeTx()
    {
        tx.success();
        tx.close();
    }

    @Override
    public void close()
    {
        closeTx();
        progressListener.done();
    }

    public int total()
    {
        return total;
    }

    public BatchTransaction withIntermediarySize( int intermediarySize )
    {
        this.intermediarySize = intermediarySize;
        return this;
    }

    public BatchTransaction withProgress( ProgressListener progressListener )
    {
        this.progressListener = progressListener;
        this.progressListener.started();
        return this;
    }
}
