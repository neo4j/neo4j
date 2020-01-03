/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.internal;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.forced_kernel_id;

public class KernelDataTest
{
    private final Collection<Kernel> kernels = new HashSet<>();
    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( pageCacheRule );

    @After
    public void tearDown()
    {
        Iterator<Kernel> kernelIterator = kernels.iterator();
        while ( kernelIterator.hasNext() )
        {
            Kernel kernel = kernelIterator.next();
            kernelIterator.remove();
            kernel.shutdown();
        }
    }

    @Test
    public void shouldGenerateUniqueInstanceIdentifiers()
    {
        // given
        Kernel kernel1 = new Kernel( null );

        // when
        Kernel kernel2 = new Kernel( null );

        // then
        assertNotNull( kernel1.instanceId() );
        assertNotNull( kernel2.instanceId() );
        assertNotEquals( kernel1.instanceId(), kernel2.instanceId() );
    }

    @Test
    public void shouldReuseInstanceIdentifiers()
    {
        // given
        Kernel kernel = new Kernel( null );
        String instanceId = kernel.instanceId();
        kernel.shutdown();

        // when
        kernel = new Kernel( null );

        // then
        assertEquals( instanceId, kernel.instanceId() );
    }

    @Test
    public void shouldAllowConfigurationOfInstanceId()
    {
        // when
        Kernel kernel = new Kernel( "myInstance" );

        // then
        assertEquals( "myInstance", kernel.instanceId() );
    }

    @Test
    public void shouldGenerateInstanceIdentifierWhenNullConfigured()
    {
        // when
        Kernel kernel = new Kernel( null );

        // then
        assertEquals( kernel.instanceId(), kernel.instanceId().trim() );
        assertTrue( kernel.instanceId().length() > 0 );
    }

    @Test
    public void shouldGenerateInstanceIdentifierWhenEmptyStringConfigured()
    {
        // when
        Kernel kernel = new Kernel( "" );

        // then
        assertEquals( kernel.instanceId(), kernel.instanceId().trim() );
        assertTrue( kernel.instanceId().length() > 0 );
    }

    @Test
    public void shouldNotAllowMultipleInstancesWithTheSameConfiguredInstanceId()
    {
        // given
        new Kernel( "myInstance" );

        // when
        try
        {
            new Kernel( "myInstance" );
            fail( "should have thrown exception" );
        }
        // then
        catch ( IllegalStateException e )
        {
            assertEquals( "There is already a kernel started with unsupported.dbms.kernel_id='myInstance'.", e.getMessage() );
        }
    }

    @Test
    public void shouldAllowReuseOfConfiguredInstanceIdAfterShutdown()
    {
        // given
        new Kernel( "myInstance" ).shutdown();

        // when
        Kernel kernel = new Kernel( "myInstance" );

        // then
        assertEquals( "myInstance", kernel.instanceId() );
    }

    private class Kernel extends KernelData
    {
        Kernel( String desiredId )
        {
            super( fileSystemRule.get(), pageCacheRule.getPageCache( fileSystemRule.get() ),
                    new File( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ), Config.defaults( forced_kernel_id, desiredId), mock( DataSourceManager.class ) );
            kernels.add( this );
        }

        @Override
        public void shutdown()
        {
            super.shutdown();
            kernels.remove( this );
        }
    }
}
