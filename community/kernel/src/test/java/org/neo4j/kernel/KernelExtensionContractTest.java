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

import java.util.Collections;
import java.util.Map;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.*;

/**
 * Base class for testing a {@link KernelExtension}. The base test cases in this
 * class verifies that a extension upholds the {@link KernelExtension} contract.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 *
 * @param <S> The type of the state for the {@link KernelExtension}.
 * @param <X> The {@link KernelExtension} type to test.
 */
public abstract class KernelExtensionContractTest<S, X extends KernelExtension<S>>
{
    private final Class<X> extClass;
    private final String key;
    private final TargetDirectory target;

    public KernelExtensionContractTest( String key, Class<X> extClass )
    {
        this.target = TargetDirectory.forTest( getClass() );
        this.extClass = extClass;
        this.key = key;
    }

    GraphDatabaseService graphdb( String name, boolean loadExtensions, int instance )
    {
        Map<String, String> config = configuration( true, instance );
        config.put( Config.LOAD_EXTENSIONS, Boolean.toString( loadExtensions ) );
        return new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( target.directory( name, true ).getAbsolutePath()).setConfig( config ).newGraphDatabase();
    }

    /**
     * Override to create default configuration for the {@link KernelExtension}
     * under test.
     *
     * @param shouldLoad <code>true</code> if configuration that makes the
     *            extension load should be created, <code>false</code> if
     *            configuration that makes the extension not load should be
     *            created.
     * @param instance used for differentiating multiple instances that will run
     *            simultaneously.
     * @return configuration for an {@link EmbeddedGraphDatabase} that
     */
    protected Map<String, String> configuration( boolean shouldLoad, int instance )
    {
        return MapUtil.stringMap();
    }

    /**
     * Validate that two state objects are ok for use with two different
     * kernels. The default implementation just checks if the instances are
     * different.
     */
    protected boolean isOkForDifferentKernels( S state1, S state2 )
    {
        return state1 != state2;
    }

    /**
     * Validate that the state of the extension has been unloaded properly. The
     * default implementation always returns true.
     *
     * @param state the state object to validate.
     */
    protected boolean isUnloaded( S state )
    {
        return true;
    }

    /**
     * Validate that the extension is loaded with the supplied kernel. The
     * default implementation delegates to
     * {@link KernelExtension#isLoaded(KernelData)}.
     */
    protected boolean isLoadedOk( KernelData kernel )
    {
        return newInstance().isLoaded( kernel );
    }

    /**
     * Validate that the supplied state is ok loaded state for this extension.
     * The default implementation returns <code>true</code> the loaded state is
     * not null.
     */
    protected boolean isLoadedState( S state )
    {
        return state != null;
    }

    static KernelData getExtensions( GraphDatabaseService graphdb )
    {
        return ((GraphDatabaseSPI)graphdb).getKernelData();
    }

    @Test
    public void extensionShouldHavePublicNoArgConstructor() throws Exception
    {
        X instance = null;
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
    @SuppressWarnings( "null" )
    public void shouldBeAbleToLoadExtensionAsAServiceProvider() throws Exception
    {
        X instance = null;
        try
        {
            instance = loadInstance();
        }
        catch ( ClassCastException failure )
        {
            failure.printStackTrace();
            fail( "Loaded instance does not match the extension class (Exception in stderr)" );
        }
        assertNotNull( "Could not load the kernel extension with the provided key", instance );
        assertTrue( "Class of the loaded instance is a subclass of the extension class",
                instance.getClass() == extClass );
    }

    @Test
    public void differentInstancesShouldHaveEqualHashCodesAndBeEqual() throws Exception
    {
        X one = newInstance();
        X two = newInstance();
        assertEquals( "new instances have different hash codes", one.hashCode(), two.hashCode() );
        assertEquals( "new instances are not equals", one, two );

        one = loadInstance();
        two = loadInstance();
        assertEquals( "loaded instances have different hash codes", one.hashCode(), two.hashCode() );
        assertEquals( "loaded instances are not equals", one, two );

        one = loadInstance();
        two = newInstance();
        assertEquals( "loaded instance and new instance have different hash codes", one.hashCode(), two.hashCode() );
        assertEquals( "loaded instance and new instance are not equals", one, two );
    }

    @Test
    public void canLoadKernelExtension() throws Exception
    {
        GraphDatabaseService graphdb = graphdb( "graphdb", /*loadExtensions=*/true, 0 );
        try
        {
            assertTrue( "Failed to load extension", isLoadedOk( getExtensions( graphdb ) ) );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    @Test
    public void differentInstancesUseSameState() throws Exception
    {
        GraphDatabaseService graphdb = graphdb( "graphdb", true, 0 );
        try
        {
            KernelData kernel = getExtensions( graphdb );
            assertSame( newInstance().getState( kernel ), newInstance().getState( kernel ) );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    @Test
    public void sameInstanceCanLoadWithMultipleKernels() throws Exception
    {
        GraphDatabaseService graphdb1 = graphdb( "graphdb1", /*loadExtensions=*/false, 1 );
        try
        {
            GraphDatabaseService graphdb2 = graphdb( "graphdb2", /*loadExtensions=*/false, 2 );
            try
            {
                KernelData extensions1 = getExtensions( graphdb1 ), extensions2 = getExtensions( graphdb2 );
                X instance = newInstance();
                extensions1.loadExtensions( Collections.<KernelExtension<?>>singleton( instance ), StringLogger.SYSTEM );
                extensions2.loadExtensions( Collections.<KernelExtension<?>>singleton( instance ), StringLogger.SYSTEM );
                S state1 = extensions1.getState( newInstance() ), state2 = extensions2.getState( newInstance() );
                assertTrue( "Failed to load extension with first kernel", isLoadedState( state1 ) );
                assertTrue( "Failed to load extension with second kernel", isLoadedState( state2 ) );
                assertTrue( "Loaded same extension state for both kernels", isOkForDifferentKernels( state1, state2 ) );
                testUnload( "first kernel", extensions1, instance, state1 );
                testUnload( "second kernel", extensions2, instance, state2 );
            }
            finally
            {
                graphdb2.shutdown();
            }
        }
        finally
        {
            graphdb1.shutdown();
        }
    }

    private void testUnload( String kernelName, KernelData kernel, X instance, S state )
    {
        kernel.shutdown( StringLogger.SYSTEM );
        assertTrue( "Internal failure: failed to unload extension with " + kernelName, kernel.getState( instance ) == null );
        assertTrue( "Failed to unload instance with " + kernelName, isUnloaded( state ) );
    }

    private final X newInstance()
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

    protected final X loadInstance()
    {
        return extClass.cast( Service.load( KernelExtension.class, key ) );
    }
}
