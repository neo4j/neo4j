/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.test.rule.PageCacheRule;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class KernelDataTest
{

    private final PageCacheRule pageCacheRule = new PageCacheRule();
    private final DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    private final TestRule shutDownRemainingKernels = new TestRule()
    {
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
                        base.evaluate();
                    }
                    finally
                    {
                        for ( Kernel kernel : kernels.toArray( new Kernel[kernels.size()] ) )
                        {
                            kernel.shutdown();
                        }
                        kernels.clear();
                    }
                }
            };
        }
    };

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule )
            .around( pageCacheRule ).around( shutDownRemainingKernels );

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
        assertFalse( kernel1.instanceId().equals( kernel2.instanceId() ) );
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
                    new File( "graph.db" ), Config.defaults( config( desiredId ) ) );
            kernels.add( this );
        }

        @Override
        public Version version()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void shutdown()
        {
            super.shutdown();
            kernels.remove( this );
        }
    }

    private final Collection<Kernel> kernels = new HashSet<>();

    private static Map<String,String> config( String desiredId )
    {
        HashMap<String,String> config = new HashMap<>();
        if ( desiredId != null )
        {
            config.put( KernelData.forced_id.name(), desiredId );
        }
        return config;
    }

}
