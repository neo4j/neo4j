/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.graphdb.factory.module;

import java.io.File;
import java.util.function.Function;

import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.factory.AccessCapability;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.logging.internal.LogService;
import org.neo4j.time.SystemNanoClock;

public class DefaultEditionModuleDatabaseContext implements EditionDatabaseContext
{
    private final IdContextFactory idContextFactory;
    private final Function<Locks,StatementLocksFactory> statementLocksFactoryProvider;
    private final Function<File,FileSystemWatcherService> watcherServiceFactory;
    private final String databaseName;
    private final AccessCapability accessCapability;
    private final IOLimiter ioLimiter;
    private final ConstraintSemantics constraintSemantics;
    private final CommitProcessFactory commitProcessFactory;
    private final TransactionHeaderInformationFactory headerInformationFactory;
    private final SchemaWriteGuard schemaWriteGuard;
    private final long transactionStartTimeout;
    private final Function<String,TokenHolders> tokenHoldersProvider;
    private final Locks locks;
    private final DatabaseTransactionStats transactionMonitor;
    private final EditionModule editionModule;

    DefaultEditionModuleDatabaseContext( EditionModule editionModule, String databaseName )
    {
        this.databaseName = databaseName;
        this.transactionStartTimeout = editionModule.getTransactionStartTimeout();
        this.schemaWriteGuard = editionModule.schemaWriteGuard;
        this.headerInformationFactory = editionModule.headerInformationFactory;
        this.commitProcessFactory = editionModule.commitProcessFactory;
        this.constraintSemantics = editionModule.constraintSemantics;
        this.ioLimiter = editionModule.ioLimiter;
        this.accessCapability = editionModule.accessCapability;
        this.watcherServiceFactory = editionModule.watcherServiceFactory;
        this.idContextFactory = editionModule.idContextFactory;
        this.tokenHoldersProvider = editionModule.tokenHoldersProvider;
        this.locks = editionModule.locksSupplier.get();
        this.statementLocksFactoryProvider = editionModule.statementLocksFactoryProvider;
        this.transactionMonitor = editionModule.createTransactionMonitor();
        this.editionModule = editionModule;
    }

    @Override
    public DatabaseIdContext createIdContext()
    {
        return idContextFactory.createIdContext( databaseName );
    }

    @Override
    public TokenHolders createTokenHolders()
    {
        return tokenHoldersProvider.apply( databaseName );
    }

    @Override
    public Function<File,FileSystemWatcherService> getWatcherServiceFactory()
    {
        return watcherServiceFactory;
    }

    @Override
    public AccessCapability getAccessCapability()
    {
        return accessCapability;
    }

    @Override
    public IOLimiter getIoLimiter()
    {
        return ioLimiter;
    }

    @Override
    public ConstraintSemantics getConstraintSemantics()
    {
        return constraintSemantics;
    }

    @Override
    public CommitProcessFactory getCommitProcessFactory()
    {
        return commitProcessFactory;
    }

    @Override
    public TransactionHeaderInformationFactory getHeaderInformationFactory()
    {
        return headerInformationFactory;
    }

    @Override
    public SchemaWriteGuard getSchemaWriteGuard()
    {
        return schemaWriteGuard;
    }

    @Override
    public long getTransactionStartTimeout()
    {
        return transactionStartTimeout;
    }

    @Override
    public Locks createLocks()
    {
        return locks;
    }

    @Override
    public StatementLocksFactory createStatementLocksFactory()
    {
        return statementLocksFactoryProvider.apply( locks );
    }

    @Override
    public DatabaseTransactionStats createTransactionMonitor()
    {
        return transactionMonitor;
    }

    @Override
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( SystemNanoClock clock, LogService logService, Config config )
    {
        return editionModule.createDatabaseAvailabilityGuard( databaseName, clock, logService, config );
    }
}
