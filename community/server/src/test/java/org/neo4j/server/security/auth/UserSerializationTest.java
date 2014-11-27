/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.server.security.auth;

import org.junit.Test;

import java.util.List;

import org.neo4j.kernel.impl.util.Charsets;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UserSerializationTest
{
    @Test
    public void shouldSerializeAndDeserialize() throws Exception
    {
        // Given
        UserSerialization serialization = new UserSerialization();

        List<User> users = asList(
                new User( "Steve", "12345", Privileges.ADMIN, new Credentials( "SomeSalt", "SomeAlgo", "1234321" ),
                        false ),
                new User( "Bob", "54321", Privileges.ADMIN, new Credentials( "OtherSalt", "OtherAlgo", "0987654" ),
                        false ) );

        // When
        byte[] serialized = serialization.serialize( users );

        // Then
        assertThat( serialization.deserializeUsers( serialized ), equalTo( users ) );
    }

    /**
     * This is a future-proofing test. If you come here because you've made changes to the serialization format,
     * this is your reminder to make sure to build this is in a backwards compatible way.
     */
    @Test
    public void shouldReadV1SerializationFormat() throws Exception
    {
        // Given
        UserSerialization serialization = new UserSerialization();

        // When
        List<User> deserialized = serialization.deserializeUsers(
                ("Steve:12345:SomeAlgo,1234321,SomeSalt:\n" +
                 "Bob::OtherAlgo,0987654,OtherSalt:password_change_required") .getBytes( Charsets.UTF_8 ) );

        // Then
        assertThat( deserialized, equalTo( asList(
                new User( "Steve", "12345", Privileges.ADMIN, new Credentials( "SomeSalt", "SomeAlgo", "1234321" ),
                        false ),
                new User( "Bob", null, Privileges.ADMIN, new Credentials( "OtherSalt", "OtherAlgo", "0987654" ),
                        true ) ) ) );
    }
}