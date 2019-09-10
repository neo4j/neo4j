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
package org.neo4j.kernel.recovery;

import org.neo4j.dbms.database.DatabaseStartAbortedException;
import org.neo4j.kernel.database.DatabaseStartupController;
import org.neo4j.kernel.database.NamedDatabaseId;

import static java.util.Objects.requireNonNull;
import static org.neo4j.kernel.database.DatabaseStartupController.NEVER_ABORT;

public class RecoveryStartupChecker
{
    public static final RecoveryStartupChecker EMPTY_CHECKER = new NeverCanceledChecker();

    private final DatabaseStartupController databaseStartupController;
    private final NamedDatabaseId namedDatabaseId;

    public RecoveryStartupChecker( DatabaseStartupController databaseStartupController, NamedDatabaseId namedDatabaseId )
    {
        this.databaseStartupController = requireNonNull( databaseStartupController );
        this.namedDatabaseId = namedDatabaseId;
    }

    void checkIfCanceled() throws DatabaseStartAbortedException
    {
        if ( databaseStartupController.shouldAbort( namedDatabaseId ) )
        {
            throw new DatabaseStartAbortedException( namedDatabaseId );
        }
    }

    private static class NeverCanceledChecker extends RecoveryStartupChecker
    {
        private NeverCanceledChecker()
        {
            super( NEVER_ABORT, null );
        }

        @Override
        void checkIfCanceled()
        {

        }
    }
}
