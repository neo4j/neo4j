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
package org.neo4j.dbms.database;

import static java.lang.String.format;
import static org.neo4j.function.ThrowingAction.executeAll;
import static org.neo4j.kernel.database.NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID;
import static org.neo4j.kernel.database.NamedDatabaseId.SYSTEM_DATABASE_NAME;

import java.util.Optional;
import org.neo4j.dbms.api.DatabaseManagementException;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation;
import org.neo4j.gqlstatus.GqlMessageParams;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * System and default database manged only by lifecycles.
 */
public final class DatabaseLifecycles {
    private final DatabaseRepository<StandaloneDatabaseContext> databaseRepository;
    private final String defaultDatabaseName;
    private final DatabaseContextFactory<StandaloneDatabaseContext, Optional<?>> databaseContextFactory;
    private final Log log;

    public DatabaseLifecycles(
            DatabaseRepository<StandaloneDatabaseContext> databaseRepository,
            String defaultDatabaseName,
            DatabaseContextFactory<StandaloneDatabaseContext, Optional<?>> databaseContextFactory,
            LogProvider logProvider) {
        this.databaseRepository = databaseRepository;
        this.defaultDatabaseName = defaultDatabaseName;
        this.databaseContextFactory = databaseContextFactory;
        this.log = logProvider.getLog(getClass());
    }

    public Lifecycle systemDatabaseStarter() {
        return new SystemDatabaseStarter();
    }

    public Lifecycle defaultDatabaseStarter() {
        return new DefaultDatabaseStarter();
    }

    public Lifecycle allDatabaseShutdown() {
        return new AllDatabaseStopper();
    }

    private StandaloneDatabaseContext systemContext() {
        return databaseRepository
                .getDatabaseContext(NAMED_SYSTEM_DATABASE_ID)
                .orElseThrow(() -> new DatabaseNotFoundException("database not found: " + SYSTEM_DATABASE_NAME));
    }

    private Optional<StandaloneDatabaseContext> defaultContext() {
        return databaseRepository.getDatabaseContext(defaultDatabaseName);
    }

    private synchronized void initialiseDefaultDatabase() {
        var defaultDatabaseId = databaseRepository
                .databaseIdRepository()
                .getByName(defaultDatabaseName)
                .orElseThrow(() -> new DatabaseNotFoundException("Default database not found: " + defaultDatabaseName));
        if (databaseRepository.getDatabaseContext(defaultDatabaseId).isPresent()) {
            throw new DatabaseManagementException(
                    "Cannot initialize " + defaultDatabaseId + " because it already exists");
        }
        var context = createDatabase(defaultDatabaseId);
        startDatabase(context);
    }

    private StandaloneDatabaseContext createDatabase(NamedDatabaseId namedDatabaseId) {
        log.info("Creating '%s'.", namedDatabaseId);
        checkDatabaseLimit(namedDatabaseId);
        StandaloneDatabaseContext databaseContext = databaseContextFactory.create(namedDatabaseId, Optional.empty());
        databaseRepository.add(namedDatabaseId, databaseContext);
        return databaseContext;
    }

    private void stopDatabase(StandaloneDatabaseContext context) {
        var namedDatabaseId = context.database().getNamedDatabaseId();
        // Make sure that any failure (typically database panic) that happened until now is not interpreted as shutdown
        // failure
        context.clearFailure();
        try {
            log.info("Stopping '%s'.", namedDatabaseId);
            Database database = context.database();

            database.stop();
            log.info("Stopped '%s' successfully.", namedDatabaseId);
        } catch (Throwable t) {
            log.error("Failed to stop " + namedDatabaseId, t);
            context.fail(new DatabaseManagementException(
                    format("An error occurred! Unable to stop `%s`.", namedDatabaseId), t));
        }
    }

    private void startDatabase(StandaloneDatabaseContext context) {
        var namedDatabaseId = context.database().getNamedDatabaseId();
        try {
            log.info("Starting '%s'.", namedDatabaseId);
            Database database = context.database();
            database.start();
        } catch (Throwable t) {
            log.error("Failed to start " + namedDatabaseId, t);
            var gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N40)
                    .withParam(GqlMessageParams.namedDatabaseId, namedDatabaseId.name())
                    .withClassification(ErrorClassification.DATABASE_ERROR)
                    .build();
            context.fail(new UnableToStartDatabaseException(
                    gql, format("An error occurred! Unable to start `%s`.", namedDatabaseId), t));
        }
    }

    private void checkDatabaseLimit(NamedDatabaseId namedDatabaseId) {
        if (databaseRepository.registeredDatabases().size() >= 2) {
            throw new DatabaseManagementException(
                    "Default database already exists. Fail to create another: " + namedDatabaseId);
        }
    }

    private class SystemDatabaseStarter extends LifecycleAdapter {
        @Override
        public void init() {
            createDatabase(NAMED_SYSTEM_DATABASE_ID);
        }

        @Override
        public void start() {
            startDatabase(systemContext());
        }
    }

    private class AllDatabaseStopper extends LifecycleAdapter {
        @Override
        public void stop() throws Exception {
            var standaloneDatabaseContext = defaultContext();
            standaloneDatabaseContext.ifPresent(DatabaseLifecycles.this::stopDatabase);

            StandaloneDatabaseContext systemContext = systemContext();
            stopDatabase(systemContext);

            executeAll(
                    () -> standaloneDatabaseContext.ifPresent(this::throwIfUnableToStop),
                    () -> throwIfUnableToStop(systemContext));
        }

        private void throwIfUnableToStop(StandaloneDatabaseContext ctx) {

            if (!ctx.isFailed()) {
                return;
            }

            // If we have not been able to start the database instance, then
            // we do not want to add a compounded error due to not being able
            // to stop the database.
            if (ctx.failureCause() instanceof UnableToStartDatabaseException) {
                return;
            }

            throw new DatabaseManagementException(
                    "Failed to stop " + ctx.database().getNamedDatabaseId().name() + " database.", ctx.failureCause());
        }
    }

    private class DefaultDatabaseStarter extends LifecycleAdapter {
        @Override
        public void start() {
            initialiseDefaultDatabase();
        }
    }
}
