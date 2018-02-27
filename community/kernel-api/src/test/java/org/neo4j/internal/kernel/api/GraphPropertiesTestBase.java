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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.graphdb.PropertyContainer;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.values.storable.Values.NO_VALUE;
import static org.neo4j.values.storable.Values.stringValue;

@SuppressWarnings( "Duplicates" )
public abstract class GraphPropertiesTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldBeAbleToWriteNewGraphProperty() throws Exception
    {
        int prop;
        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( graphProperties().getProperty( "prop" ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldBeAbleToReadExistingGraphProperties() throws Exception
    {
        int prop1, prop2, prop3;
        try ( Transaction tx = session.beginTransaction() )
        {
            prop1 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop1" );
            prop2 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop2" );
            prop3 = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop3" );
            tx.dataWrite().graphSetProperty( prop1, stringValue( "hello" ) );
            tx.dataWrite().graphSetProperty( prop2, stringValue( "world" ) );
            tx.dataWrite().graphSetProperty( prop3, stringValue( "etc" ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction();
              PropertyCursor cursor = cursors.allocatePropertyCursor() )
        {
            tx.dataRead().graphProperties( cursor );

            assertTrue( cursor.next() );
            assertThat( cursor.propertyKey(), equalTo( prop1 ) );
            assertThat( cursor.propertyValue(), equalTo( stringValue( "hello" ) ) );

            assertTrue( cursor.next() );
            assertThat( cursor.propertyKey(), equalTo( prop2 ) );
            assertThat( cursor.propertyValue(), equalTo( stringValue( "world" ) ) );

            assertTrue( cursor.next() );
            assertThat( cursor.propertyKey(), equalTo( prop3 ) );
            assertThat( cursor.propertyValue(), equalTo( stringValue( "etc" ) ) );

            assertFalse( cursor.next() );
        }
    }

    protected abstract PropertyContainer graphProperties();
}
