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
package org.neo4j.kernel.impl.core;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.subprocess.BeforeDebuggedTest;
import org.neo4j.test.subprocess.BreakPoint;
import org.neo4j.test.subprocess.BreakpointHandler;
import org.neo4j.test.subprocess.BreakpointTrigger;
import org.neo4j.test.subprocess.DebugInterface;
import org.neo4j.test.subprocess.DebuggedThread;
import org.neo4j.test.subprocess.DebuggerDeadlockCallback;
import org.neo4j.test.subprocess.EnabledBreakpoints;
import org.neo4j.test.subprocess.ForeignBreakpoints;
import org.neo4j.test.subprocess.SubProcessTestRunner;

import static org.junit.Assert.assertEquals;

/**
 * Regression test for a data race between loading properties from the disk and modifying properties.
 *
 * @author Tobias Lindaaker <tobias.lindaaker@neotechnology.com>
 */
@ForeignBreakpoints( {
        @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.core.ArrayBasedPrimitive", method = "setProperties" ),
        @ForeignBreakpoints.BreakpointDef( type = "org.neo4j.kernel.impl.core.ArrayBasedPrimitive", method = "commitPropertyMaps", on = BreakPoint.Event.EXIT ) } )
@RunWith( SubProcessTestRunner.class )
@SuppressWarnings( "javadoc" )
@Ignore( "Ignored in 2.0 due to half-way refactoring moving properties into kernel API. " +
        "Unignore and change appropriately when it's done" )
public class TestPropertyCachePoisoning
{
    @Test
    @EnabledBreakpoints( { "setProperties", "removeProperty" } )
    public void raceBetweenPropertyReaderAndPropertyWriter()
    {
        final Node first = new TX<Node>()
        {
            @Override
            Node perform()
            {
                Node node = graphdb.createNode();
                node.setProperty( "key", "value" );
                return node;
            }
        }.result();
        clearCache();
        new TxThread()
        {
            @Override
            void perform()
            {
                removeProperty( first, "key" );
            }
        };
        first.getProperty( "key", null );
        final Node second = new TX<Node>()
        {
            @Override
            Node perform()
            {
                Node node = graphdb.createNode();
                node.setProperty( "key", "other" );
                return node;
            }
        }.result();
        new TX<Void>()
        {
            @Override
            Void perform()
            {
                first.removeProperty( "key" );
                return null;
            }
        };
        clearCache();
        assertEquals( "other", second.getProperty( "key" ) );
    }

    private static DebuggedThread readerThread, removerThread;

    @BeforeDebuggedTest
    public static void resetThreadReferences()
    {
        readerThread = removerThread = null;
    }

    @BreakpointTrigger
    void removeProperty( Node node, String key )
    {
        node.removeProperty( key );
    }

    @BreakpointHandler( "removeProperty" )
    public static void handleRemoveProperty(
            @BreakpointHandler( "commitPropertyMaps" ) BreakPoint exitCommit, DebugInterface di )
    {
        if ( readerThread == null )
        {
            removerThread = di.thread().suspend( null );
        }
        exitCommit.enable();
    }

    @BreakpointHandler( "setProperties" )
    public static void handleSetProperties( BreakPoint self, DebugInterface di )
    {
        self.disable();
        if ( removerThread != null )
        {
            removerThread.resume();
            removerThread = null;
        }
        readerThread = di.thread().suspend( DebuggerDeadlockCallback.RESUME_THREAD );
    }

    @BreakpointHandler( value = "commitPropertyMaps" )
    public static void exitCommitPropertyMaps( BreakPoint self, DebugInterface di )
    {
        self.disable();
        readerThread.resume();
        readerThread = null;
    }

    private void clearCache()
    {
        graphdb.getDependencyResolver().resolveDependency( Caches.class ).clear();
    }

    private abstract class TxThread
    {
        public TxThread()
        {
            new Thread()
            {
                @Override
                public void run()
                {
                    Transaction tx = graphdb.beginTx();
                    try
                    {
                        perform();
                        tx.success();
                    }
                    finally
                    {
                        tx.finish();
                    }
                }
            }.start();
        }

        abstract void perform();
    }

    private abstract class TX<T>
    {
        private final T value;

        TX()
        {
            Transaction tx = graphdb.beginTx();
            try
            {
                value = perform();
                tx.success();
            }
            finally
            {
                tx.finish();
            }
        }

        abstract T perform();

        final T result()
        {
            return value;
        }
    }

    private GraphDatabaseAPI graphdb;

    @Before
    public void startGraphdb()
    {
        graphdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase();
    }

    @After
    public void stopGraphdb()
    {
        try
        {
            if ( graphdb != null )
            {
                graphdb.shutdown();
            }
        }
        finally
        {
            graphdb = null;
        }
    }
}
