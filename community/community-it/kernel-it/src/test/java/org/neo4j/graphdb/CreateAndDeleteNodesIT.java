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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;

import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

@ImpermanentDbmsExtension
class CreateAndDeleteNodesIT
{
    @Inject
    private GraphDatabaseService db;

    enum RelTypes implements RelationshipType
    {
        ASD
    }

    @Test
    void creatingAndDeletingEntitiesShouldNotThrow()
    {
        // Given
        Node myNode;

        // When
        try ( Transaction bobTransaction = db.beginTx() )
        {
            myNode = bobTransaction.createNode();
            myNode.setProperty( "Name", "Bob" );

            myNode.createRelationshipTo( bobTransaction.createNode(), RelTypes.ASD );
            bobTransaction.commit();
        }

        // When
        GraphDatabaseServiceCleaner.cleanupAllRelationshipsAndNodes( db );
    }
}
