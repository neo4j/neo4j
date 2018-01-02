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
package org.neo4j.kernel.impl.api;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class KernelSchemaStateStoreTest
{
    @Test
    public void should_apply_updates_correctly()
    {
        // GIVEN
        stateStore.apply( MapUtil.stringMap( "key", "created_value" ) );

        // WHEN
        String result = stateStore.get( "key" );

        // THEN
        assertEquals( "created_value", result );
    }


    @Test
    public void should_flush()
    {
        // GIVEN
        stateStore.apply( MapUtil.stringMap( "key", "created_value" ) );

        // WHEN
        stateStore.clear();

        // THEN
        String result = stateStore.get( "key" );
        assertEquals( null, result );

        // AND ALSO
        logProvider.assertExactly(
                inLog( KernelSchemaStateStore.class ).info( "Schema state store has been cleared." )
        );
    }

    private KernelSchemaStateStore stateStore;
    private AssertableLogProvider logProvider = new AssertableLogProvider();

    @Before
    public void before()
    {
        this.stateStore = new KernelSchemaStateStore( logProvider );
    }
}
