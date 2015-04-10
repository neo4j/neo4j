/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.value;

import org.junit.Test;

import org.neo4j.driver.Identity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

import static org.neo4j.driver.internal.Identities.identity;

public class IdentityValueTest
{

    @Test
    public void testValueAsIdentity() throws Exception
    {
        // Given
        Identity id = identity( "node/1" );
        IdentityValue value = new IdentityValue( id );

        // Then
        assertThat( value.asIdentity(), equalTo( id ) );
    }

    @Test
    public void testIsIdentity() throws Exception
    {
        // Given
        Identity id = identity( "node/1" );
        IdentityValue value = new IdentityValue( id );

        // Then
        assertThat( value.isIdentity(), equalTo( true ) );
    }

    @Test
    public void testEquals() throws Exception
    {
        // Given
        Identity id = identity( "node/1" );
        IdentityValue firstValue = new IdentityValue( id );
        IdentityValue secondValue = new IdentityValue( id );

        // Then
        assertThat( firstValue, equalTo( secondValue ) );
    }

    @Test
    public void testHashCode() throws Exception
    {
        // Given
        Identity id = identity( "node/1" );
        IdentityValue value = new IdentityValue( id );

        // Then
        assertThat( value.hashCode(), notNullValue() );
    }
}