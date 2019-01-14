/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

public interface DatabaseEditionContext
{
    DatabaseIdContext getIdContext();

    TokenHolders createTokenHolders();

    Function<File,FileSystemWatcherService> getWatcherServiceFactory();

    AccessCapability getAccessCapability();

    IOLimiter getIoLimiter();

    ConstraintSemantics getConstraintSemantics();

    CommitProcessFactory getCommitProcessFactory();

    TransactionHeaderInformationFactory getHeaderInformationFactory();

    SchemaWriteGuard getSchemaWriteGuard();

    long getTransactionStartTimeout();

    Locks createLocks();

    StatementLocksFactory createStatementLocksFactory();

    DatabaseTransactionStats createTransactionMonitor();

    DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( SystemNanoClock clock, LogService logService, Config config );
}
