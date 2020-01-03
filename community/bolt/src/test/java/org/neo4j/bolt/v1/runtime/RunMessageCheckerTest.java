/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.neo4j.bolt.v1.messaging.request.RunMessage;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.bolt.v1.runtime.RunMessageChecker.isBegin;
import static org.neo4j.bolt.v1.runtime.RunMessageChecker.isCommit;
import static org.neo4j.bolt.v1.runtime.RunMessageChecker.isRollback;

class RunMessageCheckerTest
{
    @ParameterizedTest
    @ValueSource( strings = {"begin", "BEGIN", "   begin   ", "   BeGiN ;   ", " begin     ;"} )
    void shouldCheckBegin( String statement )
    {
        assertTrue( isBegin( new RunMessage( statement ) ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"commit", "COMMIT", "   commit   ", "   CoMmIt ;   ", " commiT     ;"} )
    void shouldCheckCommit( String statement )
    {
        assertTrue( isCommit( new RunMessage( statement ) ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"rollback", "ROLLBACK", "   rollback   ", "   RoLlBaCk ;   ", " Rollback     ;"} )
    void shouldCheckRollback( String statement )
    {
        assertTrue( isRollback( new RunMessage( statement ) ) );
    }

    @ParameterizedTest
    @ValueSource( strings = {"RETURN 1", "CREATE ()", "MATCH (n) RETURN n", "RETURN 'Hello World!'"} )
    void shouldCheckStatement( String statement )
    {
        assertFalse( isBegin( new RunMessage( statement ) ) );
        assertFalse( isCommit( new RunMessage( statement ) ) );
        assertFalse( isRollback( new RunMessage( statement ) ) );
    }
}
