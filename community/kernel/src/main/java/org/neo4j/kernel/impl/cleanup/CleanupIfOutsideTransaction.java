/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.cleanup;

import javax.transaction.Status;
import javax.transaction.SystemException;

import org.neo4j.helpers.Thunk;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class CleanupIfOutsideTransaction implements Thunk<Boolean>
{
    private final AbstractTransactionManager txManager;

    public CleanupIfOutsideTransaction( AbstractTransactionManager txManager )
    {
        if ( null == txManager )
            throw new IllegalArgumentException( "null txManager is not allowed" );
        this.txManager = txManager;
    }

    @Override
    public Boolean evaluate()
    {
        try
        {
            // Return true if we aren't in a tx, so that the cleanup
            // service gets it.
            return txManager.getStatus() == Status.STATUS_NO_TRANSACTION;
        }
        catch ( SystemException e )
        {
            // TODO correct to just return false here?
            return false;
        }
    }
}