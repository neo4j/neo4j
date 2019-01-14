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
package org.neo4j.server.security.auth;

import org.junit.Test;

import java.util.List;

import org.neo4j.kernel.impl.security.Credential;
import org.neo4j.kernel.impl.security.User;
import org.neo4j.string.UTF8;

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
                new User.Builder( "Mike", Credential.forPassword( "1234321" ) ).withFlag( "not_as_nice" ).build(),
                new User.Builder( "Steve", Credential.forPassword( "1234321" ) ).build(),
                new User.Builder( "steve.stevesson@WINDOMAIN", Credential.forPassword( "1234321" ) ).build(),
                new User.Builder( "Bob", Credential.forPassword( "0987654" ) ).build()
            );

        // When
        byte[] serialized = serialization.serialize( users );

        // Then
        assertThat( serialization.deserializeRecords( serialized ), equalTo( users ) );
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
        byte[] salt1 = new byte[] { (byte) 0xa5, (byte) 0x43 };
        byte[] hash1 = new byte[] { (byte) 0xfe, (byte) 0x00, (byte) 0x56, (byte) 0xc3, (byte) 0x7e };
        byte[] salt2 = new byte[] { (byte) 0x34, (byte) 0xa4 };
        byte[] hash2 = new byte[] { (byte) 0x0e, (byte) 0x1f, (byte) 0xff, (byte) 0xc2, (byte) 0x3e };

        // When
        List<User> deserialized = serialization.deserializeRecords( UTF8.encode( "Mike:SHA-256,FE0056C37E,A543:\n" +
                "Steve:SHA-256,FE0056C37E,A543:nice_guy,password_change_required\n" +
                "Bob:SHA-256,0E1FFFC23E,34A4:password_change_required\n" ) );

        // Then
        assertThat( deserialized, equalTo(
                asList( new User.Builder( "Mike", new Credential( salt1, hash1 ) ).build(),
                        new User.Builder( "Steve", new Credential( salt1, hash1 ) )
                            .withRequiredPasswordChange( true ).withFlag("nice_guy").build(),
                        new User.Builder( "Bob", new Credential( salt2, hash2 ) )
                                .withRequiredPasswordChange( true )
                                .build()
        ) ) );
    }
}
