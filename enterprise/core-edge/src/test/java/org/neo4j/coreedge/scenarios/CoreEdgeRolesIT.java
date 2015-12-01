/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.scenarios;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.test.TargetDirectory;

public class CoreEdgeRolesIT
{
    public final
    @Rule
    TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Rule
    public ExpectedException exceptionMatcher = ExpectedException.none();

    private Cluster cluster;

    @After
    public void shutdown()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void edgeServersShouldRefuseWrites() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 1);
        GraphDatabaseService db = cluster.findAnEdgeServer();
        Transaction tx = db.beginTx();
        db.createNode();

        // then
        exceptionMatcher.expect( TransactionFailureException.class );

        // when
        tx.success();
        tx.close();
    }
}
