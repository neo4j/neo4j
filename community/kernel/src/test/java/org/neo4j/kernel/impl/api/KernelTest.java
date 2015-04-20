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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class KernelTest
{
    @Test
    public void shouldNotAllowCreationOfConstraintsWhenInHA() throws Exception
    {
        //noinspection deprecation
        GraphDatabaseAPI db = new FakeHaDatabase();
        ThreadToStatementContextBridge stmtBridge =
                db.getDependencyResolver().resolveDependency( ThreadToStatementContextBridge.class );

        try ( Transaction ignored = db.beginTx() )
        {
            Statement statement = stmtBridge.instance();

            try
            {
                statement.schemaWriteOperations().uniquenessConstraintCreate( 1, 1 );
                fail( "expected exception here" );
            }
            catch ( InvalidTransactionTypeKernelException e )
            {
                assertThat( e.getMessage(), containsString( "HA" ) );
            }
        }

        db.shutdown();
    }

    @SuppressWarnings("deprecation")
    class FakeHaDatabase extends ImpermanentGraphDatabase
    {
        @Override
        public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
        {
            throw new InvalidTransactionTypeKernelException(
                    "Creation or deletion of constraints is not possible while running in a HA cluster. " +
                    "In order to do that, please restart in non-HA mode and propagate the database copy to " +
                    "all slaves" );
        }
    }
}
