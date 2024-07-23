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
package org.neo4j.graphdb.factory.module.id;

import static org.neo4j.configuration.GraphDatabaseSettings.db_format;

import java.util.function.Function;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.internal.id.AbstractBufferingIdGeneratorFactory;
import org.neo4j.internal.id.BufferedIdController;
import org.neo4j.internal.id.BufferingIdGeneratorFactory;
import org.neo4j.internal.id.IdController;
import org.neo4j.internal.id.IdGeneratorFactory;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;

public class DefaultIdContextFactory implements IdContextFactory {
    private final JobScheduler jobScheduler;
    private final IdGeneratorFactoryCreator idFactoryProvider;
    private final Function<IdGeneratorFactory, IdGeneratorFactory> factoryWrapper;
    private final LogService logService;

    protected DefaultIdContextFactory(
            JobScheduler jobScheduler,
            IdGeneratorFactoryCreator idFactoryProvider,
            Function<IdGeneratorFactory, IdGeneratorFactory> factoryWrapper,
            LogService logService) {
        this.jobScheduler = jobScheduler;
        this.idFactoryProvider = idFactoryProvider;
        this.factoryWrapper = factoryWrapper;
        this.logService = logService;
    }

    protected AbstractBufferingIdGeneratorFactory wrapWithBufferingFactory(
            IdGeneratorFactory idGeneratorFactory, DatabaseConfig databaseConfig) {
        return new BufferingIdGeneratorFactory(idGeneratorFactory);
    }

    @Override
    public DatabaseIdContext createIdContext(
            NamedDatabaseId namedDatabaseId,
            CursorContextFactory contextFactory,
            DatabaseConfig databaseConfig,
            boolean allocationInitiallyEnabled) {
        return createBufferingIdContext(
                idFactoryProvider,
                jobScheduler,
                contextFactory,
                namedDatabaseId,
                databaseConfig,
                allocationInitiallyEnabled);
    }

    private DatabaseIdContext createBufferingIdContext(
            IdGeneratorFactoryCreator idGeneratorFactoryProvider,
            JobScheduler jobScheduler,
            CursorContextFactory contextFactory,
            NamedDatabaseId namedDatabaseId,
            DatabaseConfig databaseConfig,
            boolean allocationInitiallyEnabled) {
        var idGeneratorFactory = idGeneratorFactoryProvider.apply(namedDatabaseId, allocationInitiallyEnabled);
        var bufferingIdGeneratorFactory = wrapWithBufferingFactory(idGeneratorFactory, databaseConfig);
        var bufferingController = createBufferedIdController(
                bufferingIdGeneratorFactory,
                jobScheduler,
                contextFactory,
                namedDatabaseId.name(),
                logService,
                databaseConfig);
        return createIdContext(bufferingIdGeneratorFactory, bufferingController);
    }

    private DatabaseIdContext createIdContext(IdGeneratorFactory idGeneratorFactory, IdController idController) {
        return new DatabaseIdContext(factoryWrapper.apply(idGeneratorFactory), idController);
    }

    private static BufferedIdController createBufferedIdController(
            AbstractBufferingIdGeneratorFactory idGeneratorFactory,
            JobScheduler scheduler,
            CursorContextFactory contextFactory,
            String databaseName,
            LogService logService,
            DatabaseConfig databaseConfig) {
        return new BufferedIdController(
                idGeneratorFactory, scheduler, contextFactory, databaseConfig, databaseName, logService);
    }

    protected static boolean isMultiVersion(Config databaseConfig) {
        return "multiversion".equals(databaseConfig.get(db_format));
    }
}
