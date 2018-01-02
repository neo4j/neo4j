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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.udc.UsageDataKeys.OperationalMode;

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
            Statement statement = stmtBridge.get();

            try
            {
                statement.schemaWriteOperations().uniquePropertyConstraintCreate( 1, 1 );
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
        protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            new CommunityFacadeFactory()
            {
                @Override
                protected PlatformModule createPlatform( File storeDir, Map<String, String> params,
                        Dependencies dependencies, GraphDatabaseFacade graphDatabaseFacade,
                        OperationalMode operationalMode )
                {
                    return new ImpermanentPlatformModule( storeDir, params, dependencies, graphDatabaseFacade );
                }

                @Override
                protected EditionModule createEdition( PlatformModule platformModule )
                {
                    return new CommunityEditionModule( platformModule )
                    {
                        @Override
                        protected SchemaWriteGuard createSchemaWriteGuard()
                        {
                            return new SchemaWriteGuard()
                            {
                                @Override
                                public void assertSchemaWritesAllowed() throws InvalidTransactionTypeKernelException
                                {
                                    throw new InvalidTransactionTypeKernelException(
                                            "Creation or deletion of constraints is not possible while running in a HA cluster. " +
                                            "In order to do that, please restart in non-HA mode and propagate the database copy to " +
                                            "all slaves" );
                                }
                            };
                        }
                    };
                }
            }.newFacade( storeDir, params, dependencies, this );
        }
    }
}
