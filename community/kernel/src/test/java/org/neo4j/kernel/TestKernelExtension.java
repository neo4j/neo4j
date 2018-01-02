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
package org.neo4j.kernel;

import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactoryContractTest;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.lifecycle.LifecycleStatus;

import static org.junit.Assert.assertEquals;

/**
 * Test the implementation of the {@link org.neo4j.kernel.extension.KernelExtensionFactory} framework. Treats the
 * framework as a black box and takes the perspective of the extension, making
 * sure that the framework fulfills its part of the contract. The parent class (
 * {@link KernelExtensionFactoryContractTest}) takes the opposite approach, it treats
 * the extension implementation as a black box to assert that it fulfills the
 * requirements stipulated by the framework.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 */
public final class TestKernelExtension extends KernelExtensionFactoryContractTest
{
    public TestKernelExtension()
    {
        super( DummyExtensionFactory.EXTENSION_ID, DummyExtensionFactory.class );
    }

    /**
     * Check that lifecycle status of extension is STARTED
     */
    @Test
    public void shouldBeStarted() throws Exception
    {
        GraphDatabaseAPI graphdb = graphdb( 0 );
        try
        {
            assertEquals( LifecycleStatus.STARTED, graphdb.getDependencyResolver().resolveDependency(
                    KernelExtensions.class ).resolveDependency( DummyExtension.class ).getStatus() );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    /**
     * Check that dependencies can be accessed
     */
    @Test
    public void dependenciesCanBeRetrieved() throws Exception
    {
        GraphDatabaseAPI graphdb = graphdb( 0 );
        try
        {
            assertEquals( graphdb.getDependencyResolver().resolveDependency( Config.class ),
                    graphdb.getDependencyResolver().resolveDependency( KernelExtensions.class ).resolveDependency(
                            DummyExtension.class ).getDependencies().getConfig() );
            assertEquals( graphdb.getDependencyResolver().resolveDependency( NeoStoreDataSource.class ),
                    graphdb.getDependencyResolver().resolveDependency( KernelExtensions.class ).resolveDependency(
                            DummyExtension.class ).getDependencies().getNeoStoreDataSource().get() );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    /**
     * Check that lifecycle status of extension is SHUTDOWN
     */
    @Test
    public void shouldBeShutdown() throws Exception
    {
        GraphDatabaseAPI graphdb = graphdb( 0 );
        graphdb.shutdown();

        assertEquals( LifecycleStatus.SHUTDOWN, graphdb.getDependencyResolver().resolveDependency( KernelExtensions
                .class ).resolveDependency( DummyExtension.class ).getStatus() );
    }
}
