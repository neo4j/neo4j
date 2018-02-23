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

import org.junit.Rule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.migrationsupport.rules.EnableRuleMigrationSupport;
import org.junit.rules.ExpectedException;

import javax.annotation.Resource;

import org.neo4j.test.extension.ImpermanentDatabaseExtension;
import org.neo4j.test.rule.ImpermanentDatabaseRule;

@EnableRuleMigrationSupport
@ExtendWith( ImpermanentDatabaseExtension.class )
public class DeleteNodeWithRelsIT
{
    @Resource
    public ImpermanentDatabaseRule db;

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldGiveHelpfulExceptionWhenDeletingNodeWithRels()
    {
        // Given
        GraphDatabaseService db = this.db.getGraphDatabaseAPI();

        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            node.createRelationshipTo( db.createNode(), RelationshipType.withName( "MAYOR_OF" ) );
            tx.success();
        }

        // And given a transaction deleting just the node
        Transaction tx = db.beginTx();
        node.delete();
        tx.success();

        // Expect
        exception.expect( ConstraintViolationException.class );
        exception.expectMessage( "Cannot delete node<" + node.getId() + ">, because it still has relationships. " +
                "To delete this node, you must first delete its relationships." );

        // When I commit
        tx.close();
    }

}
