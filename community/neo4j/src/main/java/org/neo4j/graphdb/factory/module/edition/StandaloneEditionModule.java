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
package org.neo4j.graphdb.factory.module.edition;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.database.readonly.ConfigBasedLookupFactory;
import org.neo4j.configuration.database.readonly.ConfigReadOnlyDatabaseListener;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DbmsRuntimeRepository;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.StandaloneDbmsRuntimeRepository;
import org.neo4j.dbms.database.readonly.ReadOnlyDatabases;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyDatabaseLookupFactory;
import org.neo4j.dbms.database.readonly.SystemGraphReadOnlyListener;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.InternalLogProvider;

public abstract class StandaloneEditionModule extends AbstractEditionModule {
    @Override
    public DbmsRuntimeRepository createAndRegisterDbmsRuntimeRepository(
            GlobalModule globalModule,
            DatabaseManager<?> databaseManager,
            Dependencies dependencies,
            DbmsRuntimeSystemGraphComponent dbmsRuntimeSystemGraphComponent) {
        var dbmsRuntimeRepository =
                new StandaloneDbmsRuntimeRepository(databaseManager, dbmsRuntimeSystemGraphComponent);
        globalModule
                .getTransactionEventListeners()
                .registerTransactionEventListener(SYSTEM_DATABASE_NAME, dbmsRuntimeRepository);
        return dbmsRuntimeRepository;
    }

    protected static ReadOnlyDatabases createGlobalReadOnlyChecker(
            DatabaseManager<?> databaseManager,
            Config globalConfig,
            GlobalTransactionEventListeners txListeners,
            LifeSupport globalLife,
            InternalLogProvider logProvider) {
        var systemGraphReadOnlyLookup = new SystemGraphReadOnlyDatabaseLookupFactory(databaseManager, logProvider);
        var configReadOnlyLookup = new ConfigBasedLookupFactory(globalConfig, databaseManager.databaseIdRepository());
        var globalChecker = new ReadOnlyDatabases(systemGraphReadOnlyLookup, configReadOnlyLookup);
        var configListener = new ConfigReadOnlyDatabaseListener(globalChecker, globalConfig);
        var systemGraphListener = new SystemGraphReadOnlyListener(txListeners, globalChecker);
        globalLife.add(configListener);
        globalLife.add(systemGraphListener);
        return globalChecker;
    }
}
