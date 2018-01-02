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
package org.neo4j.kernel.impl.core;

import org.junit.Rule;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.DatabaseRule;
import org.neo4j.test.ImpermanentDatabaseRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.Assert.assertObjectOrArrayEquals;

public abstract class PropertyContainerProxyTest
{
    @Rule
    public final DatabaseRule db = new ImpermanentDatabaseRule();
    @Rule
    public final CleanupRule cleanup = new CleanupRule();

    protected abstract long createPropertyContainer();

    protected abstract PropertyContainer lookupPropertyContainer( long id );

    @Test
    public void shouldListAllProperties()
    {
        // Given
        Map<String,Object> properties = new HashMap<>();
        properties.put( "boolean", true );
        properties.put( "short_string", "abc" );
        properties.put( "string", "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVW" +
                                  "XYZabcdefghijklmnopqrstuvwxyz" );
        properties.put( "long", Long.MAX_VALUE );
        properties.put( "short_array", new long[]{1, 2, 3, 4} );
        properties.put( "array", new long[]{Long.MAX_VALUE - 1, Long.MAX_VALUE - 2, Long.MAX_VALUE - 3,
                Long.MAX_VALUE - 4, Long.MAX_VALUE - 5, Long.MAX_VALUE - 6, Long.MAX_VALUE - 7,
                Long.MAX_VALUE - 8, Long.MAX_VALUE - 9, Long.MAX_VALUE - 10, Long.MAX_VALUE - 11} );

        long containerId;

        try ( Transaction tx = db.beginTx() )
        {
            containerId = createPropertyContainer();
            PropertyContainer container = lookupPropertyContainer( containerId );

            for ( Map.Entry<String,Object> entry : properties.entrySet() )
            {
                container.setProperty( entry.getKey(), entry.getValue() );
            }

            tx.success();
        }

        // When
        Map<String,Object> listedProperties;
        try ( Transaction tx = db.beginTx() )
        {
            listedProperties = lookupPropertyContainer( containerId ).getAllProperties();
            tx.success();
        }

        // Then
        assertEquals( properties.size(), listedProperties.size() );
        for ( String key : properties.keySet() )
        {
            assertObjectOrArrayEquals( properties.get( key ), listedProperties.get( key ) );
        }
    }
}
