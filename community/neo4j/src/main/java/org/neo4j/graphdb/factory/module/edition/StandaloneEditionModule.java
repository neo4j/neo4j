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
package org.neo4j.graphdb.factory.module.edition;

import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.DefaultDatabaseManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.context.EditionDatabaseComponents;
import org.neo4j.graphdb.factory.module.edition.context.StandaloneDatabaseComponents;
import org.neo4j.graphdb.factory.module.id.IdContextFactory;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.StatementLocksFactory;
import org.neo4j.logging.Log;
import org.neo4j.token.TokenHolders;

public abstract class StandaloneEditionModule extends AbstractEditionModule
{
    protected CommitProcessFactory commitProcessFactory;
    protected IdContextFactory idContextFactory;
    protected Function<DatabaseId,TokenHolders> tokenHoldersProvider;
    protected Supplier<Locks> locksSupplier;
    protected Function<Locks,StatementLocksFactory> statementLocksFactoryProvider;

    @Override
    public EditionDatabaseComponents createDatabaseComponents( DatabaseId databaseId )
    {
        return new StandaloneDatabaseComponents( this, databaseId );
    }

    public CommitProcessFactory getCommitProcessFactory()
    {
        return commitProcessFactory;
    }

    public IdContextFactory getIdContextFactory()
    {
        return idContextFactory;
    }

    public Function<DatabaseId,TokenHolders> getTokenHoldersProvider()
    {
        return tokenHoldersProvider;
    }

    public Supplier<Locks> getLocksSupplier()
    {
        return locksSupplier;
    }

    public Function<Locks,StatementLocksFactory> getStatementLocksFactoryProvider()
    {
        return statementLocksFactoryProvider;
    }

    @Override
    public DatabaseManager<?> createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, GlobalModule globalModule, Log msgLog )
    {
        return new DefaultDatabaseManager( globalModule, this, msgLog, graphDatabaseFacade );
    }
}
