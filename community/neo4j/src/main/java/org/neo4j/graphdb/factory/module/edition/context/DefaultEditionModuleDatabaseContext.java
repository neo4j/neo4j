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
package org.neo4j.graphdb.factory.module.edition.context;

import java.io.File;
import java.util.function.Function;

import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.factory.module.edition.DefaultEditionModule;
import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
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

public class DefaultEditionModuleDatabaseContext implements DatabaseEditionContext
{
    private final Function<File,FileSystemWatcherService> watcherServiceFactory;
    private final String databaseName;
    private final AccessCapability accessCapability;
    private final IOLimiter ioLimiter;
    private final ConstraintSemantics constraintSemantics;
    private final CommitProcessFactory commitProcessFactory;
    private final TransactionHeaderInformationFactory headerInformationFactory;
    private final SchemaWriteGuard schemaWriteGuard;
    private final long transactionStartTimeout;
    private final TokenHolders tokenHolders;
    private final Locks locks;
    private final DatabaseTransactionStats transactionMonitor;
    private final AbstractEditionModule editionModule;
    private final DatabaseIdContext idContext;
    private final StatementLocksFactory statementLocksFactory;

    public DefaultEditionModuleDatabaseContext( DefaultEditionModule editionModule, String databaseName )
    {
        this.databaseName = databaseName;
        this.transactionStartTimeout = editionModule.getTransactionStartTimeout();
        this.schemaWriteGuard = editionModule.getSchemaWriteGuard();
        this.headerInformationFactory = editionModule.getHeaderInformationFactory();
        this.commitProcessFactory = editionModule.getCommitProcessFactory();
        this.constraintSemantics = editionModule.getConstraintSemantics();
        this.ioLimiter = editionModule.getIoLimiter();
        this.accessCapability = editionModule.getAccessCapability();
        this.watcherServiceFactory = editionModule.getWatcherServiceFactory();
        this.idContext = editionModule.getIdContextFactory().createIdContext( databaseName );
        this.tokenHolders = editionModule.getTokenHoldersProvider().apply( databaseName );
        this.locks = editionModule.getLocksSupplier().get();
        this.statementLocksFactory = editionModule.getStatementLocksFactoryProvider().apply( locks );
        this.transactionMonitor = editionModule.createTransactionMonitor();
        this.editionModule = editionModule;
    }

    @Override
    public DatabaseIdContext getIdContext()
    {
        return idContext;
    }

    @Override
    public TokenHolders createTokenHolders()
    {
        return tokenHolders;
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
        return statementLocksFactory;
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
