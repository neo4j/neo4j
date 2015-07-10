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
package org.neo4j.index;

import static org.junit.Assert.fail;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.embedded.CommunityTestGraphDatabase;
import org.neo4j.embedded.GraphDatabase;
import org.neo4j.graphdb.Transaction;

public class TestSingleTransactionLegacyIndexConstraints
{
    private GraphDatabase db;

    @Before
    public void before()
    {
        db = CommunityTestGraphDatabase.openEphemeral();
    }

    @After
    public void after()
    {
        db.shutdown();
    }

    @Test
    public void alteringMoreThan63IndexesInASingleTransactionShouldLeadToIllegalStateException() throws Exception
    {
        // Given
        // A database with at least 64 indexes
        try ( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 64; i++ )
            {
                db.index().forNodes( "foo" + i );
            }
            tx.success();
        }

        // When
        // We try to alter 64 of them in a single tx
        try( Transaction tx = db.beginTx() )
        {
            for ( int i = 0; i < 63; i++ )
            {
                db.index().forNodes( "foo" + i ).add( db.createNode(), "aKey", "aValue" );
            }

            // Then
            // it should lead to an IllegalStateException
            try
            {
                db.index().forNodes( "foo" + 63 ).add( db.createNode(), "aKey", "aValue" );
                fail( "Altering more than 63 legacy indexes in a single tx should not be allowed" );
            }
            catch( IllegalStateException e )
            {
                // fantastic
            }
            tx.success();
        }
    }
}
