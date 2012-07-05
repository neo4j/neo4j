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
package org.neo4j.kernel;

import static java.lang.Integer.MAX_VALUE;
import static java.lang.String.valueOf;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.collection.MapUtil.stringMap;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.Transaction;

public class GuardPerformanceImpact
{

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
        final InternalAbstractGraphDatabase db = prepare( false );
        try
        {
            final long start = currentTimeMillis();

            createData( db );

            return currentTimeMillis() - start;
        } finally
        {
            cleanup( db );
        }
    }

    private static InternalAbstractGraphDatabase prepare( boolean insertGuard ) throws IOException
    {
        File tmpFile = File.createTempFile( "neo4j-test", "" );
        tmpFile.delete();
        return new EmbeddedGraphDatabase( tmpFile.getCanonicalPath(),
                stringMap( "enable_execution_guard", valueOf( insertGuard ) ) );
    }

    private static void createData( final InternalAbstractGraphDatabase db )
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

    private static void cleanup( final InternalAbstractGraphDatabase db )
    {
        db.shutdown();
        deleteFiles( new File( db.getStoreDir() ) );
    }

    private static void deleteFiles( final File directory )
    {
        final File[] files = directory.listFiles();
        if ( files != null )
        {
            for ( File file : files )
            {
                deleteFiles( file );
            }
        }
        directory.delete();
    }

    private static long guardEnabled() throws IOException
    {
        final InternalAbstractGraphDatabase db = prepare( true );
        try
        {
            final long start = currentTimeMillis();

            createData( db );
            return currentTimeMillis() - start;
        } finally
        {
            cleanup( db );
        }
    }

    private static long guardEnabledAndActiveOpsCount() throws IOException
    {
        final InternalAbstractGraphDatabase db = prepare( true );
        try
        {
            final long start = currentTimeMillis();

            db.getGuard().startOperationsCount( MAX_VALUE );

            createData( db );

            return currentTimeMillis() - start;
        } finally
        {
            cleanup( db );
        }
    }

    private static long guardEnabledAndActiveTimeout() throws IOException
    {
        final InternalAbstractGraphDatabase db = prepare( true );
        try
        {
            final long start = currentTimeMillis();

            db.getGuard().startTimeout( MAX_VALUE );

            createData( db );

            return currentTimeMillis() - start;
        } finally
        {
            cleanup( db );
        }
    }
}
