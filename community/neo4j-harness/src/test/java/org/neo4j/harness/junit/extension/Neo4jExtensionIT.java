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
package org.neo4j.harness.junit.extension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.Neo4j;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith( Neo4jExtension.class )
class Neo4jExtensionIT
{

    @BeforeEach
    void setUp( Neo4j neo4j, GraphDatabaseService databaseService )
    {
        assertNotNull( neo4j );
        assertNotNull( databaseService );
    }

    @Test
    void neo4jAvailable( Neo4j neo4j )
    {
        assertNotNull( neo4j );
        assertThat( HTTP.GET( neo4j.httpURI().toString() ).status(), equalTo( 200 ) );
    }

    @Test
    void graphDatabaseServiceIsAvailable( GraphDatabaseService databaseService )
    {
        assertNotNull( databaseService );
        assertDoesNotThrow( () ->
        {
            try ( Transaction transaction = databaseService.beginTx() )
            {
                databaseService.createNode();
                transaction.success();
            }
        } );
    }
}
