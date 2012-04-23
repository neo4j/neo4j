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

import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;

import static org.junit.Assert.*;

/**
 * Test the implementation of the {@link KernelExtension} framework. Treats the
 * framework as a black box and takes the perspective of the extension, making
 * sure that the framework fulfills its part of the contract. The parent class (
 * {@link KernelExtensionContractTest}) takes the opposite approach, it treats
 * the extension implementation as a black box to assert that it fulfills the
 * requirements stipulated by the framework.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
 */
public final class TestKernelExtension extends KernelExtensionContractTest<DummyExtension.State, DummyExtension>
{
    public TestKernelExtension()
    {
        super( DummyExtension.EXTENSION_ID, DummyExtension.class );
    }

    /**
     * This tests the facilities needed for testing kernel extensions, more than
     * anything else.
     */
    @Test
    public void canDisableLoadingKernelExtensions() throws Exception
    {
        GraphDatabaseService graphdb = graphdb( "graphdb", /*loadExtensions=*/false, 0 );
        try
        {
            assertFalse( "Extensions were loaded despite configured not to",
                    new DummyExtension().isLoaded( getExtensions( graphdb ) ) );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    /**
     * We know which instance we loaded, we are asserting that the framework
     * will give us that same instance when we ask for the state.
     */
    @Test
    public void shouldRetrieveSameLoadedStateObjectWhenRequested() throws Exception
    {
        GraphDatabaseService graphdb = graphdb( "graphdb", /*loadExtensions=*/true, 0 );
        try
        {
            DummyExtension.State state = new DummyExtension().getState( getExtensions( graphdb ) );
            assertNotNull( state );
            assertSame( DummyExtension.lastState, state );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    @Test
    public void differentExtensionsCanHaveDifferentState() throws Exception
    {
        GraphDatabaseService graphdb = graphdb( "graphdb", /*loadExtensions=*/true, 0 );
        try
        {
            KernelData kernel = getExtensions( graphdb );
            DummyExtension.State state = new DummyExtension().getState( kernel ), other = new OtherExtension().getState( kernel );
            assertNotNull( state );
            assertNotNull( other );
            assertNotSame( state, other );
        }
        finally
        {
            graphdb.shutdown();
        }
    }

    @Override
    protected boolean isUnloaded( DummyExtension.State state )
    {
        return state.unloaded;
    }
}
