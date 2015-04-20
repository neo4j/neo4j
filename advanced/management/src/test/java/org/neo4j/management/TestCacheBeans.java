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
package org.neo4j.management;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.jmx.impl.JmxKernelExtension;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.core.Caches;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.*;
import static org.neo4j.graphdb.DynamicLabel.label;

public class TestCacheBeans
{
    private GraphDatabaseAPI graphDb;
    private Collection<Cache> caches;

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule( );

    @Before
    public synchronized void startGraphDb()
    {
        graphDb = dbRule.getGraphDatabaseAPI();
        caches = graphDb.getDependencyResolver().resolveDependency( JmxKernelExtension.class ).
                getManagementBeans( Cache.class );
    }

    @After
    public synchronized void stopGraphDb()
    {
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
        graphDb = null;
    }

    @Test
    public void canAccessCacheBeans() throws Exception
    {
        assertNotNull( "no cache beans", caches );
        assertFalse( "no cache beans", caches.isEmpty() );
    }

    @Test
    public void canMeasureSizeOfCache() throws Exception
    {
        Label label = label( "ANode" );
        long[] before = get( CacheBean.CACHE_SIZE );
        long nodeId;
        try ( Transaction tx = graphDb.beginTx() )
        {
            nodeId = graphDb.createNode( label ).getId();
            tx.success();
        }
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.getNodeById( nodeId ).hasLabel(label);
        }

        assertChanged( "cache size not updated", before, get( CacheBean.CACHE_SIZE ) );
    }

    @Test
    public void canMeasureAmountsOfHitsAndMisses() throws Exception
    {
        Label label = label( "ANode" );
        try(Transaction tx = graphDb.beginTx())
        {
            graphDb.createNode(label);
            tx.success();
        }

        graphDb.getDependencyResolver().resolveDependency( Caches.class ).clear();

        long[] hits = get( CacheBean.HIT_COUNT ), miss = get( CacheBean.MISS_COUNT );
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graphDb.getNodeById(0).hasLabel( label );
            graphDb.getNodeById(0).hasLabel( label );
            transaction.success();
        }
        assertChanged( "hit count not updated", hits, get( CacheBean.HIT_COUNT ) );
        assertChanged( "miss count not updated", miss, get( CacheBean.MISS_COUNT ) );
    }

    private void assertChanged( String message, long[] before, long[] after )
    {
        if ( Arrays.equals( before, after ) )
        {
            fail( message + ", before=" + Arrays.toString( before ) + ", after=" + Arrays.toString( after ) );
        }
    }

    private enum CacheBean
    {
        CACHE_SIZE
        {
            @Override
            long get( Cache bean )
            {
                return bean.getCacheSize();
            }
        },
        HIT_COUNT
        {
            @Override
            long get( Cache bean )
            {
                return bean.getHitCount();
            }
        },
        MISS_COUNT
        {
            @Override
            long get( Cache bean )
            {
                return bean.getMissCount();
            }
        };

        abstract long get( Cache bean );
    }

    private long[] get( CacheBean accessor )
    {
        long[] result = new long[caches.size()];
        Iterator<Cache> iter = caches.iterator();
        for ( int i = 0; i < result.length; i++ )
        {
            result[i] = accessor.get( iter.next() );
        }
        return result;
    }
}
