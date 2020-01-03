/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v3.runtime.integration;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.BoltStateMachineFactoryImpl;
import org.neo4j.bolt.security.auth.Authentication;
import org.neo4j.bolt.security.auth.BasicAuthentication;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.udc.UsageData;

public class SessionExtension implements BeforeEachCallback, AfterEachCallback
{
    private GraphDatabaseAPI gdb;
    private BoltStateMachineFactoryImpl boltFactory;
    private List<BoltStateMachine> runningMachines = new ArrayList<>();
    private boolean authEnabled;

    private Authentication authentication( AuthManager authManager, UserManagerSupplier userManagerSupplier )
    {
        return new BasicAuthentication( authManager, userManagerSupplier );
    }

    public BoltStateMachine newMachine( long version, BoltChannel boltChannel )
    {
        if ( boltFactory == null )
        {
            throw new IllegalStateException( "Cannot access test environment before test is running." );
        }
        BoltStateMachine machine = boltFactory.newStateMachine( version, boltChannel );
        runningMachines.add( machine );
        return machine;
    }

    @Override
    public void beforeEach( ExtensionContext extensionContext )
    {
        Map<Setting<?>,String> config = new HashMap<>();
        config.put( GraphDatabaseSettings.auth_enabled, Boolean.toString( authEnabled ) );
        gdb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().newImpermanentDatabase( config );
        DependencyResolver resolver = gdb.getDependencyResolver();
        Authentication authentication = authentication( resolver.resolveDependency( AuthManager.class ),
                resolver.resolveDependency( UserManagerSupplier.class ) );
        boltFactory = new BoltStateMachineFactoryImpl(
                resolver.resolveDependency( DatabaseManager.class ),
                new UsageData( null ),
                authentication,
                Clock.systemUTC(),
                Config.defaults(),
                NullLogService.getInstance()
        );
    }

    @Override
    public void afterEach( ExtensionContext extensionContext )
    {
        try
        {
            if ( runningMachines != null )
            {
                IOUtils.closeAll( runningMachines );
            }
        }
        catch ( Throwable e )
        {
            e.printStackTrace();
        }

        gdb.shutdown();
    }
}
