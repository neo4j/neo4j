/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.com.ComException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.ha.com.master.Master;

/**
 * Thrown upon network communication failures, when taking or releasing distributed locks in HA.
 */
public class DistributedLockFailureException extends TransientTransactionFailureException
{
    public DistributedLockFailureException( String message, Master master, ComException cause )
    {
        super( message + " (for master instance " + master + "). The most common causes of this exception are " +
               "network failures, or master-switches where the failing transaction was started before the last " +
               "master election.", cause );
    }
}
