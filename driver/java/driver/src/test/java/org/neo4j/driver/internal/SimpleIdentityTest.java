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
package org.neo4j.driver.internal;

import org.junit.Test;

import org.neo4j.driver.Identity;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class SimpleIdentityTest
{

    @Test
    public void shouldBeAbleToCompareIdentities() throws Throwable
    {
        // Given
        Identity firstIdentity = new SimpleIdentity( "node/1" );
        Identity secondIdentity = new SimpleIdentity( "node/1" );

        // Then
        assertThat( firstIdentity, equalTo( secondIdentity ) );

    }

    @Test
    public void hashCodeShouldNotBeNull() throws Throwable
    {
        // Given
        Identity identity = new SimpleIdentity( "node/1" );

        // Then
        assertThat( identity.hashCode(), notNullValue() );

    }

    @Test
    public void shouldBeAbleToCastIdentityToString() throws Throwable
    {
        // Given
        Identity identity = new SimpleIdentity( "node/1" );

        // Then
        assertThat( identity.toString(), equalTo( "node/1" ) );

    }

}