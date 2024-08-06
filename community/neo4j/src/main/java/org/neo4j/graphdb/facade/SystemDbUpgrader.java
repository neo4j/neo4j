/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.facade;

import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;

import java.util.concurrent.TimeUnit;
import org.neo4j.commandline.dbms.MigrateStoreCommand;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.database.DbmsRuntimeSystemGraphComponent;
import org.neo4j.dbms.database.SystemGraphComponents;
import org.neo4j.dbms.systemgraph.ContextBasedSystemDatabaseProvider;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.migration.MigrationEditionModuleFactory;
import org.neo4j.graphdb.factory.module.edition.migration.SystemDatabaseMigrator;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.security.provider.SecurityProvider;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.storemigration.VisibleMigrationProgressMonitorFactory;
import org.neo4j.kernel.internal.event.GlobalTransactionEventListeners;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
import org.neo4j.procedure.impl.GlobalProceduresRegistry;

/**
 * Tool used by {@link MigrateStoreCommand} to upgrade system db.
 */
public class SystemDbUpgrader {
    private SystemDbUpgrader() {}

    /**
     * Start System db in a minimal context and upgrade:
     * <ol>
     * <li>Upgrade all system graph components</li>
     * <li>Wait for indexes to come online.</li>
     * </ol>
     * @param editionFactory             {@link MigrationEditionModuleFactory} factory method for creating edition module.
     * @param systemDatabaseMigrator     {@link SystemDatabaseMigrator} edition specific additional steps during migration.
     * @param config                     {@link Config} configuration.
     * @param logProvider                {@link InternalLogProvider} where progress will be reported.
     * @param systemDbStartupLogProvider {@link InternalLogProvider} where system db startup will be reported.
     * @param eventListener              {@link DatabaseEventListener} event listener that will be registered with global module.
     */
    public static void upgrade(
            MigrationEditionModuleFactory editionFactory,
            SystemDatabaseMigrator systemDatabaseMigrator,
            Config config,
            InternalLogProvider logProvider,
            InternalLogProvider systemDbStartupLogProvider,
            DatabaseEventListener eventListener)
            throws Exception {
        var log = logProvider.getLog(SystemDbUpgrader.class);
        var progressMonitor = VisibleMigrationProgressMonitorFactory.forSystemUpgrade(log);
        progressMonitor.started(3);
        var bootstrapProgress = progressMonitor.startSection("Bootstrap");
        var graphDatabaseDependencies = GraphDatabaseDependencies.newDependencies()
                .extensions(Iterables.empty())
                .databaseEventListeners(Iterables.iterable(eventListener));
        var globalModule = new GlobalModule(config, DbmsInfo.TOOL, false, graphDatabaseDependencies) {
            @Override
            protected LogService createLogService(InternalLogProvider userLogProvider, boolean daemonMode) {
                return new SimpleLogService(systemDbStartupLogProvider);
            }

            @Override
            protected GlobalTransactionEventListeners createGlobalTransactionEventListeners() {
                return GlobalTransactionEventListeners.NULL;
            }
        };
        var edition = editionFactory.apply(globalModule);
        var globalDependencies = globalModule.getGlobalDependencies();
        var globalLife = globalModule.getGlobalLife();
        globalModule.getGlobalDependencies().satisfyDependency(new GlobalProceduresRegistry());

        var systemGraphComponentsBuilder = new SystemGraphComponents.DefaultBuilder();
        var dbmsRuntimeSystemGraphComponent = new DbmsRuntimeSystemGraphComponent(globalModule.getGlobalConfig());
        systemGraphComponentsBuilder.register(dbmsRuntimeSystemGraphComponent);
        edition.registerSystemGraphComponents(systemGraphComponentsBuilder, globalModule);
        globalDependencies.satisfyDependency(edition.getSystemGraphComponents());

        var databaseContextProvider = edition.createDatabaseContextProvider(globalModule);
        var systemDatabaseProvider = new ContextBasedSystemDatabaseProvider(databaseContextProvider);
        edition.createGlobalReadOnlyChecker(
                systemDatabaseProvider, databaseContextProvider.databaseIdRepository(), globalModule);
        edition.bootstrapQueryRouterServices(null);
        edition.registerDatabaseInitializers(globalModule, systemDatabaseProvider);

        edition.createDefaultDatabaseResolver(systemDatabaseProvider);
        globalDependencies.satisfyDependency(edition.getDefaultDatabaseResolver());

        edition.createSecurityModule(globalModule);
        SecurityProvider securityProvider = edition.getSecurityProvider();
        globalDependencies.satisfyDependencies(securityProvider.authManager());

        var dbmsVersionProvider = edition.createAndRegisterDbmsRuntimeRepository(
                globalModule, databaseContextProvider, globalDependencies, dbmsRuntimeSystemGraphComponent);
        globalDependencies.satisfyDependency(dbmsVersionProvider);

        globalLife.start();

        var systemContext = databaseContextProvider
                .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow(() -> new IllegalStateException("Could not start System database for upgrade."));
        var systemDb = systemContext.databaseFacade();
        bootstrapProgress.close();

        // Wait for indexes to come online
        var indexPopulationProgress = progressMonitor.startSection("Index population");
        try (var tx = systemDb.beginTx()) {
            tx.schema().awaitIndexesOnline(1, TimeUnit.HOURS);
        } catch (IllegalStateException e) {
            try {
                globalLife.shutdown();
            } catch (Exception toSuppress) {
                throw Exceptions.chain(e, toSuppress);
            }
            throw e;
        }
        indexPopulationProgress.close();

        // Remove topology graph object which are not needed
        try (var tx = systemDb.beginTx()) {
            systemDatabaseMigrator.performAdditionalSystemDatabaseMigrationSteps(
                    systemDb, tx, globalModule.getGlobalClock(), log);
            tx.commit();
        } catch (IllegalStateException e) {
            try {
                globalLife.shutdown();
            } catch (Exception toSuppress) {
                throw Exceptions.chain(e, toSuppress);
            }
            throw e;
        }

        globalLife.shutdown();
        progressMonitor.completed();
    }
}
