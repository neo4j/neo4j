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
package org.neo4j.dbms.database;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static java.util.Objects.requireNonNull;

public class StandaloneDatabaseContext implements DatabaseContext
{
    private final Database database;
    private final GraphDatabaseFacade databaseFacade;
    private volatile Throwable failureCause;

    public StandaloneDatabaseContext( Database database )
    {
        requireNonNull( database );
        this.database = database;
        this.databaseFacade = database.getDatabaseFacade();
    }

    @Override
    public Database database()
    {
        return database;
    }

    @Override
    public GraphDatabaseFacade databaseFacade()
    {
        return databaseFacade;
    }

    public void fail( Throwable failureCause )
    {
        this.failureCause = failureCause;
    }

    public boolean isFailed()
    {
        return failureCause != null;
    }

    public Throwable failureCause()
    {
        return failureCause;
    }
}
