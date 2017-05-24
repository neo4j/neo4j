/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.Map;

import org.neo4j.expirable.Expirable;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.logging.AssertableLogProvider;

import static org.junit.Assert.assertEquals;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class DatabaseSchemaStateTest
{
    @Test
    public void should_apply_updates_correctly()
    {
        // GIVEN
        stateStore.apply( map( "key", ExpirableStringContainer.of( "created_value" ) ) );

        // WHEN
        String result = stateStore.get( "key" );

        // THEN
        assertEquals( "created_value", result );
    }

    @Test
    public void should_flush()
    {
        // GIVEN
        stateStore.apply( map( "key", ExpirableStringContainer.of( "created_value" ) ) );

        // WHEN
        stateStore.clear();

        // THEN
        String result = stateStore.get( "key" );
        assertEquals( null, result );

        // AND ALSO
        logProvider.assertExactly(
                inLog( DatabaseSchemaState.class ).debug( "Schema state store has been cleared." )
        );
    }

    private DatabaseSchemaState stateStore;
    private final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Before
    public void before()
    {
        this.stateStore = new DatabaseSchemaState( logProvider );
    }

    private static Map<String,Expirable> map( String key, Expirable value )
    {
        return MapUtil.entry( key, value ).create();
    }

    private static class ExpirableStringContainer implements Expirable
    {

        private String value;
        private boolean expired;

        static ExpirableStringContainer of( String value )
        {
            return new ExpirableStringContainer( value );
        }

        private ExpirableStringContainer( String value )
        {
            this.value = value;
        }

        public String getValue()
        {
            return value;
        }

        @Override
        public void expire()
        {
            expired = true;
        }

        @Override
        public boolean isExpired()
        {
            return expired;
        }
    }
}
