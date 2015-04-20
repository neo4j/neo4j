/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.tooling;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.ImpermanentDatabaseRule;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.helpers.collection.Iterables.toList;

public class GlobalGraphOperationsIT
{

    @Rule public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @Test
    public void shouldListAllPropertyKeys() throws Exception
    {
        // Given
        GraphDatabaseService db = dbRule.getGraphDatabaseAPI();
        try( Transaction tx = db.beginTx() )
        {
            db.createNode().setProperty( "myProperty", 12);
            tx.success();
        }

        GlobalGraphOperations gg = GlobalGraphOperations.at( db );

        // When
        try( Transaction _ = db.beginTx() )
        {
            assertThat( toList( gg.getAllPropertyKeys() ), equalTo( asList( "myProperty" ) ) );
        }
    }

}
