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
package org.neo4j.graphdb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Resource;

import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

@ExtendWith( ImpermanentDatabaseExtension.class )
public class CreateAndDeleteNodesIT
{
    @Resource
    private ImpermanentDatabaseRule dbRule;

    enum RelTypes implements RelationshipType
    {
        ASD
    }

    @Test
    void addingALabelUsingAValidIdentifierShouldSucceed()
    {
        // Given
        GraphDatabaseService dataBase = dbRule.getGraphDatabaseAPI();
        Node myNode;

        // When
        try ( Transaction bobTransaction = dataBase.beginTx() )
        {
            myNode = dataBase.createNode();
            myNode.setProperty( "Name", "Bob" );

            myNode.createRelationshipTo( dataBase.createNode(), RelTypes.ASD );
            bobTransaction.success();
        }

        // When
        GraphDatabaseServiceCleaner.cleanupAllRelationshipsAndNodes( dataBase );
    }
}
