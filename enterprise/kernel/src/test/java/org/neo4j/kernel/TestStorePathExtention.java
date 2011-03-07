/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

import org.junit.Test;

public class TestStorePathExtention extends
        KernelExtensionContractTest<StorePathState, StorePathExtension1>
{
    public TestStorePathExtention()
    {
        super( StorePathExtension1.EXTENSION_ID, StorePathExtension1.class );
    }

    @Test
    public void doTheTest()
    {
        EmbeddedGraphDatabase graph1 = graphdb( "graph1", true, 0 );
        StorePathState graph1state1 = new StorePathExtension1().getState( getExtensions( graph1 ) );
        StorePathState graph1state2 = new StorePathExtension2().getState( getExtensions( graph1 ) );

        EmbeddedGraphDatabase graph2 = graphdb( "graph2", true, 1 );
        StorePathState graph2state1 = new StorePathExtension1().getState( getExtensions( graph2 ) );
        StorePathState graph2state2 = new StorePathExtension2().getState( getExtensions( graph2 ) );

        // Different instances, different states
        assertNotSame( graph1state1, graph2state1 );
        assertNotSame( graph1state1, graph2state2 );
        assertNotSame( graph1state2, graph2state1 );
        assertNotSame( graph1state2, graph2state2 );
        assertNotSame( graph1state1, graph1state2 );
        assertNotSame( graph2state1, graph2state2 );

        String fromGraph1State1 = graph1state1.path;
        assertEquals( graph1.getStoreDir(), fromGraph1State1 );

        String fromGraph1State2 = graph1state2.path;
        assertEquals( graph1.getStoreDir(), fromGraph1State2 );

        String fromGraph2State1 = graph2state1.path;
        assertEquals( graph2.getStoreDir(), fromGraph2State1 );

        String fromGraph2State2 = graph2state2.path;
        assertEquals( graph2.getStoreDir(), fromGraph2State2 );
    }

    @Override
    protected boolean isUnloaded( StorePathState state )
    {
        return state.path == null;
    }
}
