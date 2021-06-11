/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.messages;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

/**
 * validate exception messages to prevent accidental changes
 */
public class MessageUtilTest
{
    @Test
    void createNodeDenied()
    {
        Assertions.assertThat( MessageUtil.createNodeWithLabelsDenied( "label", "db", "userDesc" ) )
                  .isEqualTo( "Create node with labels 'label' on database 'db' is not allowed for userDesc." );
    }

    @Test
    void withUser()
    {
        Assertions.assertThat( MessageUtil.withUser( "user", "mode" ) ).isEqualTo( "user 'user' with mode" );
    }

    @Test
    void overridenMode()
    {
        Assertions.assertThat( MessageUtil.overridenMode( "origin", "wrapping" ) )
                  .isEqualTo( "origin overridden by wrapping" );
    }

    @Test
    void restrictedMode()
    {
        Assertions.assertThat( MessageUtil.restrictedMode( "origin", "wrapping" ) )
                  .isEqualTo( "origin restricted to wrapping" );
    }

    @Test
    void authDisabled()
    {
        Assertions.assertThat( MessageUtil.authDisabled( "mode" ) ).isEqualTo( "AUTH_DISABLED with mode" );
    }

    @Test
    void StandardMode()
    {
        Assertions.assertThat( MessageUtil.standardMode( new HashSet<>() ) ).isEqualTo( "no roles" );
        Assertions.assertThat( MessageUtil.standardMode( Set.of( "role1") ) ).isEqualTo( "roles [role1]" );
        Assertions.assertThat( MessageUtil.standardMode( Set.of( "role1", "role2") ) ).isEqualTo( "roles [role1, role2]" );

    }
}
