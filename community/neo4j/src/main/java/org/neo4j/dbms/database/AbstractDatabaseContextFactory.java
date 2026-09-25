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

import static org.neo4j.configuration.GraphDatabaseInternalSettings.snapshot_query;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.system_database_format;
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.kernel.api.Kernel;
import org.neo4j.kernel.database.CursorContextFactorySupplier;
import org.neo4j.kernel.database.IdContextFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.context.TransactionVersionContextSupplier;
import org.neo4j.kernel.impl.core.DefaultLabelIdCreator;
import org.neo4j.kernel.impl.core.DefaultPropertyTokenCreator;
import org.neo4j.kernel.impl.core.DefaultRelationshipTypeCreator;
import org.neo4j.token.CreatingTokenHolder;
import org.neo4j.token.TokenCreator;
import org.neo4j.token.TokenHolders;

public abstract class AbstractDatabaseContextFactory<CONTEXT, OPTIONS>
        implements DatabaseContextFactory<CONTEXT, OPTIONS> {
    protected final GlobalModule globalModule;
    protected final IdContextFactory idContextFactory;

    public AbstractDatabaseContextFactory(GlobalModule globalModule, IdContextFactory idContextFactory) {
        this.globalModule = globalModule;
        this.idContextFactory = idContextFactory;
    }

    public static TokenHolders createTokenHolderProvider(Supplier<Kernel> kernelSupplier) {
        return new TokenHolders(
                new CreatingTokenHolder(createPropertyKeyCreator(kernelSupplier), TYPE_PROPERTY_KEY),
                new CreatingTokenHolder(createLabelIdCreator(kernelSupplier), TYPE_LABEL),
                new CreatingTokenHolder(createRelationshipTypeCreator(kernelSupplier), TYPE_RELATIONSHIP_TYPE));
    }

    protected static TokenCreator createRelationshipTypeCreator(Supplier<Kernel> kernelSupplier) {
        return new DefaultRelationshipTypeCreator(kernelSupplier);
    }

    protected static TokenCreator createPropertyKeyCreator(Supplier<Kernel> kernelSupplier) {
        return new DefaultPropertyTokenCreator(kernelSupplier);
    }

    protected static TokenCreator createLabelIdCreator(Supplier<Kernel> kernelSupplier) {
        return new DefaultLabelIdCreator(kernelSupplier);
    }

    protected void setSystemConfig(Config globalConfig, HashMap<Setting<?>, Object> settings) {
        settings.put(GraphDatabaseInternalSettings.merged_log, false);
        settings.put(db_format, globalConfig.get(system_database_format));
    }

    protected DatabaseConfig createDatabaseConfig(
            NamedDatabaseId namedDatabaseId, Map<Setting<?>, Object> databaseSpecificSettings) {
        var globalConfig = globalModule.getGlobalConfig();
        var settings = new HashMap<>(databaseSpecificSettings);
        if (namedDatabaseId.isSystemDatabase()) {
            setSystemConfig(globalConfig, settings);
        }
        return new DatabaseConfig(settings, globalConfig);
    }

    protected final CursorContextFactorySupplier createContextFactorySupplier(
            DatabaseConfig databaseConfig, NamedDatabaseId databaseId) {
        return (multiVersion) -> {
            var pageCacheTracer = globalModule.getTracers().getPageCacheTracer();
            var factory = externalVersionContextSupplierFactory(globalModule)
                    .orElse(internalVersionContextSupplierFactory(databaseConfig, multiVersion));
            return new CursorContextFactory(pageCacheTracer, factory.create(databaseId));
        };
    }

    private static Optional<VersionContextSupplier.Factory> externalVersionContextSupplierFactory(
            GlobalModule globalModule) {
        var externalDependencies = globalModule.getExternalDependencyResolver();
        return externalDependencies.resolveOptionalDependency(VersionContextSupplier.Factory.class);
    }

    private static VersionContextSupplier.Factory internalVersionContextSupplierFactory(
            DatabaseConfig databaseConfig, boolean multiVersion) {
        return databaseId -> multiVersion || databaseConfig.get(snapshot_query)
                ? new TransactionVersionContextSupplier()
                : EMPTY_CONTEXT_SUPPLIER;
    }
}
