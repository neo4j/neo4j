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
package org.neo4j.kernel.extension;

import org.junit.Rule;
import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.internal.EmbeddedGraphDatabase;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for testing a {@link org.neo4j.kernel.extension.KernelExtensionFactory}. The base test cases in this
 * class verifies that a extension upholds the {@link org.neo4j.kernel.extension.KernelExtensionFactory} contract.
 */
public abstract class KernelExtensionFactoryContractTest
{
    private final Class<? extends KernelExtensionFactory<?>> extClass;
    private final String key;

    @Rule
    public TestDirectory target = TestDirectory.testDirectory( getClass() );

    public KernelExtensionFactoryContractTest( String key, Class<? extends KernelExtensionFactory<?>> extClass )
    {
        this.extClass = extClass;
        this.key = key;
    }

    public GraphDatabaseAPI graphdb( int instance )
    {
        Map<String, String> config = configuration( true, instance );
        return (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabaseBuilder().setConfig( config ).newGraphDatabase();
    }

    /**
     * Override to create default configuration for the {@link org.neo4j.kernel.extension.KernelExtensionFactory}
     * under test.
     *
     * @param shouldLoad <code>true</code> if configuration that makes the
     *                   extension load should be created, <code>false</code> if
     *                   configuration that makes the extension not load should be
     *                   created.
     * @param instance   used for differentiating multiple instances that will run
     *                   simultaneously.
     * @return configuration for an {@link EmbeddedGraphDatabase} that
     */
    protected Map<String, String> configuration( boolean shouldLoad, int instance )
    {
        return MapUtil.stringMap();
    }

    private static KernelExtensions getExtensions( GraphDatabaseService graphdb )
    {
        return ((GraphDatabaseAPI) graphdb).getDependencyResolver().resolveDependency( KernelExtensions.class );
    }

    @Test
    public void extensionShouldHavePublicNoArgConstructor()
    {
        KernelExtensionFactory<?> instance = null;
        try
        {
            instance = newInstance();
        }
        catch ( IllegalArgumentException failure )
        {
            failure.printStackTrace();
            fail( "Contract violation: extension class must have public no-arg constructor (Exception in stderr)" );
        }
        assertNotNull( instance );
    }

    @Test
    public void shouldBeAbleToLoadExtensionAsAServiceProvider()
    {
        KernelExtensionFactory<?> instance = null;
        try
        {
            instance = loadInstance();
        }
        catch ( ClassCastException failure )
        {
            failure.printStackTrace();
            fail( "Loaded instance does not match the extension class (Exception in stderr)" );
        }

        assertNotNull( instance, "Could not load the kernel extension with the provided key" );
        assertTrue( instance.getClass() == extClass,
                "Class of the loaded instance is a subclass of the extension class" );
    }

    @Test
    public void differentInstancesShouldHaveEqualHashCodesAndBeEqual()
    {
        KernelExtensionFactory<?> one = newInstance();
        KernelExtensionFactory<?> two = newInstance();
        assertEquals( one.hashCode(), two.hashCode(), "new instances have different hash codes" );
        assertEquals( one, two, "new instances are not equals" );

        one = loadInstance();
        two = loadInstance();
        assertEquals( one.hashCode(), two.hashCode(), "loaded instances have different hash codes" );
        assertEquals( one, two, "loaded instances are not equals" );

        one = loadInstance();
        two = newInstance();
        assertEquals( one.hashCode(), two.hashCode(), "loaded instance and new instance have different hash codes" );
        assertEquals( one, two, "loaded instance and new instance are not equals" );
    }

    @Test
    public void canLoadKernelExtension()
    {
        GraphDatabaseService graphdb = graphdb( 0 );
        try
        {
            assertTrue( getExtensions( graphdb ).isRegistered( extClass ), "Failed to load extension" );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    private KernelExtensionFactory<?> newInstance()
    {
        try
        {
            return extClass.newInstance();
        }
        catch ( Exception cause )
        {
            throw new IllegalArgumentException( "Could not instantiate extension class", cause );
        }
    }

    private KernelExtensionFactory<?> loadInstance()
    {
        return extClass.cast( Service.load( KernelExtensionFactory.class, key ) );
    }
}
