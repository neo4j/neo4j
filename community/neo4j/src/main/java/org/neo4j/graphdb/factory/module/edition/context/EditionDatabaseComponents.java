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

import java.util.function.Function;

import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.io.fs.watcher.DatabaseLayoutWatcher;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.constraints.ConstraintSemantics;
import org.neo4j.kernel.impl.factory.AccessCapabilityFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.kernel.impl.query.QueryEngineProvider;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.token.TokenHolders;

public interface EditionDatabaseComponents
{
    DatabaseIdContext getIdContext();

    TokenHolders getTokenHolders();

    Function<DatabaseLayout,DatabaseLayoutWatcher> getWatcherServiceFactory();

    IOLimiter getIoLimiter();

    ConstraintSemantics getConstraintSemantics();

    CommitProcessFactory getCommitProcessFactory();

    Locks getLocks();

    StatementLocksFactory getStatementLocksFactory();

    DatabaseTransactionStats getTransactionMonitor();

    QueryEngineProvider getQueryEngineProvider();

    AccessCapabilityFactory getAccessCapabilityFactory();

    DatabaseStartupController getStartupController();
}
