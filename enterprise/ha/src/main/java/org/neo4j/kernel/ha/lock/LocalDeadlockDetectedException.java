/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.ha.lock;

import javax.transaction.Transaction;

import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.transaction.LockManager;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.info.LockInfo;

import static java.lang.String.format;

/**
 * Temporary exception to aid in driving out a nasty "lock get stuck" issue in HA. Since it's subclasses
 * {@link DeadlockDetectedException} it will be invisible to users and code that already handle such
 * deadlock exceptions and retry. This exception is thrown instead of awaiting a lock locally on a slave
 * after it was acquired on the master, since applying a lock locally after master granted it should succeed,
 * or fail; it cannot wait for another condition.
 * 
 * While this work-around is in place there is more breathing room to figure out the real problem preventing
 * some local locks to be grabbed.
 * 
 * @author Mattias Persson
 */
public class LocalDeadlockDetectedException extends DeadlockDetectedException
{
    public LocalDeadlockDetectedException( LockManager lockManager, Transaction tx, Object resource,
            LockType type )
    {
        super( constructHelpfulDiagnosticsMessage( lockManager, tx, resource, type ) );
    }

    private static String constructHelpfulDiagnosticsMessage( LockManager lockManager,
            Transaction tx, Object resource, LockType type )
    {
        StringBuilder builder = new StringBuilder( format(
                "%s tried to apply local %s lock on %s after acquired on master. Currently these locks exist:%n",
                tx, type, resource ) );
        for ( LockInfo lock : lockManager.getAllLocks() )
        {
            if ( lock.getReadCount() > 0 || lock.getWriteCount() > 0 )
            {
                builder.append( format( lock.toString() /*lock.toString includes a %n at the end*/ ) );
            }
        }
        return builder.toString();
    }
}
