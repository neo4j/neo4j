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
package org.neo4j.kernel.impl.newapi;

import java.io.File;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.TokenWrite;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.kernel.api.security.LoginContext;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.GraphDatabaseServiceCleaner;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class WriteTestSupport implements KernelAPIWriteTestSupport
{
    private GraphDatabaseService db;
    protected DatabaseManagementService managementService;

    @Override
    public void setup( File storeDir )
    {
        db = newDb( storeDir );
        try ( KernelTransaction tx = beginTransaction() )
        {
            //We are creating these dummy tokens so that that the ones that we actually use don't get
            //the value 0. Using 0 may hide bugs that are caused by default the initialization of ints
            TokenWrite tokenWrite = tx.tokenWrite();
            tokenWrite.propertyKeyCreateForName( "DO_NOT_USE", false );
            tokenWrite.labelCreateForName( "DO_NOT_USE", false );
            tokenWrite.relationshipTypeCreateForName( "DO_NOT_USE", false );
        }
        catch ( KernelException e )
        {
            throw new AssertionError( "Failed to setup database", e );
        }
    }

    protected GraphDatabaseService newDb( File storeDir )
    {
        managementService = new TestDatabaseManagementServiceBuilder( storeDir ).impermanent().build();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    @Override
    public void clearGraph()
    {
        GraphDatabaseServiceCleaner.cleanDatabaseContent( db );
    }

    @Override
    public Kernel kernelToTest()
    {
        DependencyResolver resolver = ((GraphDatabaseAPI) this.db).getDependencyResolver();
        return resolver.resolveDependency( Kernel.class );
    }

    private KernelTransaction beginTransaction() throws TransactionFailureException
    {
        return kernelToTest().beginTransaction( KernelTransaction.Type.IMPLICIT, LoginContext.AUTH_DISABLED );
    }

    @Override
    public GraphDatabaseService graphBackdoor()
    {
        return db;
    }

    @Override
    public void tearDown()
    {
        managementService.shutdown();
        db = null;
    }
}
