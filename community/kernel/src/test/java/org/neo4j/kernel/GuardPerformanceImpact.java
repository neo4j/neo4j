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
package org.neo4j.kernel;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.io.fs.FileUtils;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;

public class GuardPerformanceImpact
{
    private static String dir = forTest( GuardPerformanceImpact.class ).makeGraphDbDir().getAbsolutePath();
    private static final int RUNS = 10;
    private static final int PER_TX = 10000;
    private static final int TX = 100;

    private enum Type
    {
        without, enabled, activeTimeout, activeOpscount
    }

    public static void main( String[] args ) throws IOException
    {
        test( Type.enabled );
    }

    private static void test( final Type type ) throws IOException
    {
        switch ( type )
        {
        case without:
            for ( int i = 0; i < RUNS; i++ )
            {
                System.err.println( withoutGuard() );
            }
            break;

        case enabled:
            for ( int i = 0; i < RUNS; i++ )
            {
                System.err.println( guardEnabled() );
            }
            break;

        case activeOpscount:
            for ( int i = 0; i < RUNS; i++ )
            {
                System.err.println( guardEnabledAndActiveOpsCount() );
            }
            break;

        case activeTimeout:
            for ( int i = 0; i < RUNS; i++ )
            {
                System.err.println( guardEnabledAndActiveTimeout() );
            }
            break;
        }
    }

    private static long withoutGuard() throws IOException
    {
        final GraphDatabaseAPI db = prepare( false );
        try
        {
            final long start = currentTimeMillis();

            createData( db );

            return currentTimeMillis() - start;
        }
        finally
        {
            cleanup( db );
        }
    }

    private static GraphDatabaseAPI prepare( boolean insertGuard ) throws IOException
    {
        return (GraphDatabaseAPI) new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( dir )
                .setConfig( stringMap( "enable_execution_guard", valueOf( insertGuard ) ) ).newGraphDatabase();
    }

    private static void createData( final GraphDatabaseAPI db )
    {
        for ( int j = 0; j < TX; j++ )
        {
            final Transaction tx = db.beginTx();
            for ( int i = 0; i < PER_TX; i++ )
            {
                db.createNode();
            }
            tx.success();
            tx.finish();
        }
    }

    private static void cleanup( final GraphDatabaseAPI db ) throws IOException
    {
        db.shutdown();
        FileUtils.deleteRecursively( new File( dir ) );
    }

    private static long guardEnabled() throws IOException
    {
        final GraphDatabaseAPI db = prepare( true );
        try
        {
            final long start = currentTimeMillis();

            createData( db );
            return currentTimeMillis() - start;
        }
        finally
        {
            cleanup( db );
        }
    }

    private static long guardEnabledAndActiveOpsCount() throws IOException
    {
        final GraphDatabaseAPI db = prepare( true );
        try
        {
            final long start = currentTimeMillis();

            db.getDependencyResolver().resolveDependency( Guard.class ).startOperationsCount( MAX_VALUE );

            createData( db );

            return currentTimeMillis() - start;
        }
        finally
        {
            cleanup( db );
        }
    }

    private static long guardEnabledAndActiveTimeout() throws IOException
    {
        final GraphDatabaseAPI db = prepare( true );
        try
        {
            final long start = currentTimeMillis();

            db.getDependencyResolver().resolveDependency( Guard.class ).startTimeout( MAX_VALUE );

            createData( db );

            return currentTimeMillis() - start;
        }
        finally
        {
            cleanup( db );
        }
    }
}
