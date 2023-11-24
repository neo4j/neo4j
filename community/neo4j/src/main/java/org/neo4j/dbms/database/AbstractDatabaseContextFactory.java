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
import static org.neo4j.configuration.GraphDatabaseSettings.db_format;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.token.api.TokenHolder.TYPE_LABEL;
import static org.neo4j.token.api.TokenHolder.TYPE_PROPERTY_KEY;
import static org.neo4j.token.api.TokenHolder.TYPE_RELATIONSHIP_TYPE;

import java.util.Optional;
import java.util.function.Supplier;
import org.neo4j.configuration.DatabaseConfig;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.context.VersionContextSupplier;
import org.neo4j.kernel.api.Kernel;
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

    private static TokenCreator createRelationshipTypeCreator(Supplier<Kernel> kernelSupplier) {
        return new DefaultRelationshipTypeCreator(kernelSupplier);
    }

    private static TokenCreator createPropertyKeyCreator(Supplier<Kernel> kernelSupplier) {
        return new DefaultPropertyTokenCreator(kernelSupplier);
    }

    private static TokenCreator createLabelIdCreator(Supplier<Kernel> kernelSupplier) {
        return new DefaultLabelIdCreator(kernelSupplier);
    }

    protected final CursorContextFactory createContextFactory(
            DatabaseConfig databaseConfig, NamedDatabaseId databaseId) {
        var pageCacheTracer = globalModule.getTracers().getPageCacheTracer();
        var factory = externalVersionContextSupplierFactory(globalModule)
                .orElse(internalVersionContextSupplierFactory(databaseConfig));
        return new CursorContextFactory(pageCacheTracer, factory.create(databaseId));
    }

    private static Optional<VersionContextSupplier.Factory> externalVersionContextSupplierFactory(
            GlobalModule globalModule) {
        var externalDependencies = globalModule.getExternalDependencyResolver();
        var klass = VersionContextSupplier.Factory.class;
        if (externalDependencies.containsDependency(klass)) {
            return Optional.of(externalDependencies.resolveDependency(klass));
        }
        return Optional.empty();
    }

    private static VersionContextSupplier.Factory internalVersionContextSupplierFactory(DatabaseConfig databaseConfig) {
        return databaseId -> "multiversion".equals(databaseConfig.get(db_format)) || databaseConfig.get(snapshot_query)
                ? new TransactionVersionContextSupplier()
                : EMPTY_CONTEXT_SUPPLIER;
    }
}
