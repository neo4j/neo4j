/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.helpers.collection.MapUtil;

public class KernelSchemaStateStoreTest
{
    private KernelSchemaStateStore stateStore;

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
        stateStore.flush();

        // THEN
        String result = stateStore.get( "key" );
        assertEquals( null, result );
    }

    @Before
    public void before()
    {
        this.stateStore = new KernelSchemaStateStore();
    }
}
