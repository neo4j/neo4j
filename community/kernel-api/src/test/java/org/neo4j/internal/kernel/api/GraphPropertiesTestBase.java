/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.values.storable.Value;

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
            assertThat( testSupport.graphProperties().getProperty( "prop" ), equalTo( "hello" ) );
        }
    }

    @Test
    public void shouldBeAbleToReplaceExistingGraphProperty() throws Exception
    {
        int prop;
        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "good bye" ) ), equalTo( stringValue("hello") ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertThat( testSupport.graphProperties().getProperty( "prop" ), equalTo( "good bye" ) );
        }
    }

    @Test
    public void shouldBeAbleToRemoveExistingGraphProperty() throws Exception
    {
        int prop;
        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertThat( tx.dataWrite().graphRemoveProperty( prop ), equalTo( stringValue("hello") ) );
            tx.success();
        }

        try ( org.neo4j.graphdb.Transaction ignore = graphDb.beginTx() )
        {
            assertFalse( testSupport.graphProperties().hasProperty( "prop" ) );
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
              PropertyCursor cursor = tx.cursors().allocatePropertyCursor() )
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

    @Test
    public void shouldSeeNewGraphPropertyInTransaction() throws Exception
    {
        try ( Transaction tx = session.beginTransaction();
              PropertyCursor cursor = tx.cursors().allocatePropertyCursor() )
        {
            int prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "hello" ) ), equalTo( NO_VALUE ) );

            tx.dataRead().graphProperties( cursor );
            assertTrue( cursor.next() );
            assertThat( cursor.propertyKey(), equalTo( prop ) );
            assertThat( cursor.propertyValue(), equalTo( stringValue( "hello" ) ) );
        }
    }

    @Test
    public void shouldSeeUpdatedGraphPropertyInTransaction() throws Exception
    {
        int prop;
        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction();
              PropertyCursor cursor = tx.cursors().allocatePropertyCursor() )
        {
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "good bye" ) ),
                    equalTo( stringValue( "hello" ) ) );

            tx.dataRead().graphProperties( cursor );
            assertTrue( cursor.next() );
            assertThat( cursor.propertyKey(), equalTo( prop ) );
            assertThat( cursor.propertyValue(), equalTo( stringValue( "good bye" ) ) );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldNotSeeRemovedGraphPropertyInTransaction() throws Exception
    {
        int prop;
        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            assertThat( tx.dataWrite().graphSetProperty( prop, stringValue( "hello" ) ), equalTo( NO_VALUE ) );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction();
              PropertyCursor cursor = tx.cursors().allocatePropertyCursor() )
        {
            assertThat( tx.dataWrite().graphRemoveProperty( prop ), equalTo( stringValue( "hello" ) ) );

            tx.dataRead().graphProperties( cursor );
            assertFalse( cursor.next() );
        }
    }

    @Test
    public void shouldNotWriteWhenSettingPropertyToSameValue() throws Exception
    {
        // Given
        int prop;
        Value theValue = stringValue( "The Value" );

        try ( Transaction tx = session.beginTransaction() )
        {
            prop = tx.tokenWrite().propertyKeyGetOrCreateForName( "prop" );
            tx.dataWrite().graphSetProperty( prop, theValue );
            tx.success();
        }

        // When
        Transaction tx = session.beginTransaction();
        assertThat( tx.dataWrite().graphSetProperty( prop, theValue ), equalTo( theValue ) );
        tx.success();

        assertThat( tx.closeTransaction(), equalTo( Transaction.READ_ONLY ) );
    }
}
