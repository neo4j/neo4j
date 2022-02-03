/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.fabric;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.fabric.config.FabricConfig;
import org.neo4j.fabric.config.FabricSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.availability.UnavailableException;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.DatabaseReference;
import org.neo4j.kernel.database.DatabaseReferenceRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public abstract class FabricDatabaseManager
{
    private final boolean multiGraphEverywhere;
    private final DatabaseReferenceRepository databaseReferenceRepo;
    private final DatabaseManager<? extends DatabaseContext> databaseManager;

    public FabricDatabaseManager( FabricConfig fabricConfig, DatabaseManager<? extends DatabaseContext> databaseManager,
            DatabaseReferenceRepository databaseReferenceRepo )
    {
        this.databaseManager = databaseManager;
        this.databaseReferenceRepo = databaseReferenceRepo;
        this.multiGraphEverywhere = fabricConfig.isEnabledByDefault();
    }

    public static boolean fabricByDefault( Config config )
    {
        return config.get( FabricSettings.enabled_by_default );
    }

    public DatabaseReferenceRepository databaseReferenceRepository()
    {
        return databaseReferenceRepo;
    }

    public boolean hasMultiGraphCapabilities( String databaseNameRaw )
    {
        return multiGraphCapabilitiesEnabledForAllDatabases() || isFabricDatabase( databaseNameRaw );
    }

    public boolean multiGraphCapabilitiesEnabledForAllDatabases()
    {
        return multiGraphEverywhere;
    }

    public GraphDatabaseFacade getDatabaseFacade( String databaseNameRaw ) throws UnavailableException
    {
        var databaseContext =  databaseReferenceRepo.getInternalByName( databaseNameRaw )
                                        .map( DatabaseReference.Internal::databaseId )
                                        .flatMap( databaseManager::getDatabaseContext )
                                        .orElseThrow( databaseNotFound( databaseNameRaw ) );

        databaseContext.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
        return databaseContext.databaseFacade();
    }

    public DatabaseReference getDatabaseReference( String databaseNameRaw ) throws UnavailableException
    {
        var ref =  databaseReferenceRepo.getByName( databaseNameRaw )
                                        .orElseThrow( databaseNotFound( databaseNameRaw ) );
        var isInternal = ref instanceof DatabaseReference.Internal;
        if ( isInternal )
        {
            assertInternalDatabaseAvailable( (DatabaseReference.Internal) ref );
        }
        return ref;
    }

    private void assertInternalDatabaseAvailable( DatabaseReference.Internal databaseReference ) throws UnavailableException
    {
        var ctx = databaseManager.getDatabaseContext( databaseReference.databaseId() )
                                 .orElseThrow( databaseNotFound( databaseReference.alias().name() ) );
        ctx.database().getDatabaseAvailabilityGuard().assertDatabaseAvailable();
    }

    private static Supplier<DatabaseNotFoundException> databaseNotFound( String databaseNameRaw )
    {
        return () -> new DatabaseNotFoundException( "Database " + databaseNameRaw + " not found" );
    }

    public abstract boolean isFabricDatabasePresent();

    public abstract void manageFabricDatabases( GraphDatabaseService system, boolean update );

    public abstract boolean isFabricDatabase( String databaseNameRaw );

    public static class Community extends FabricDatabaseManager
    {
        public Community( FabricConfig fabricConfig, DatabaseManager<? extends DatabaseContext> databaseManager,
                DatabaseReferenceRepository databaseReferenceRepo )
        {
            super( fabricConfig, databaseManager, databaseReferenceRepo );
        }

        @Override
        public boolean isFabricDatabasePresent()
        {
            return false;
        }

        @Override
        public void manageFabricDatabases( GraphDatabaseService system, boolean update )
        {
            // a "Fabric" database with special capabilities cannot exist in CE
        }

        @Override
        public boolean isFabricDatabase( String databaseNameRaw )
        {
            // a "Fabric" database with special capabilities cannot exist in CE
            return false;
        }
    }
}
