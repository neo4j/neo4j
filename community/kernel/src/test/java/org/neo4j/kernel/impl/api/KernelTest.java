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
package org.neo4j.kernel.impl.api;

import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.exceptions.InvalidTransactionTypeKernelException;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.ImpermanentGraphDatabase;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

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
            KernelTransaction ktx =
                    stmtBridge.getKernelTransactionBoundToThisThread( true );
            try
            {
                ktx.schemaWrite().uniquePropertyConstraintCreate( forLabel( 1, 1 ) );
                fail( "expected exception here" );
            }
            catch ( InvalidTransactionTypeKernelException e )
            {
                assertThat( e.getMessage(), containsString( "HA" ) );
            }
        }

        db.shutdown();
    }

    @SuppressWarnings( "deprecation" )
    class FakeHaDatabase extends ImpermanentGraphDatabase
    {
        @Override
        protected void create( File storeDir, Map<String, String> params, GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            Function<PlatformModule,EditionModule> factory =
                    platformModule -> new CommunityEditionModule( platformModule )
                    {
                        @Override
                        protected SchemaWriteGuard createSchemaWriteGuard()
                        {
                            return () ->
                            {
                                throw new InvalidTransactionTypeKernelException(
                                        "Creation or deletion of constraints is not possible while running in a HA cluster. " +
                                                "In order to do that, please restart in non-HA mode and propagate the database copy" +
                                                "to all slaves" );
                            };
                        }
                    };
            new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY,  factory )
            {
                @Override
                protected PlatformModule createPlatform( File storeDir, Config config, Dependencies dependencies,
                        GraphDatabaseFacade graphDatabaseFacade )
                {
                    return new ImpermanentPlatformModule( storeDir, config, databaseInfo, dependencies,
                            graphDatabaseFacade );
                }
            }.initFacade( storeDir, params, dependencies, this );
        }
    }
}
