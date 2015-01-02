/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction;

import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.List;

import javax.transaction.Transaction;

class LockWorkerState
{
    final LockManager grabber;
    volatile boolean deadlockOnLastWait;
    final List<String> completedOperations = new ArrayList<String>();
    String doing;
    final Transaction tx = mock( Transaction.class );
    
    public LockWorkerState( LockManager grabber )
    {
        this.grabber = grabber;
    }
    
    public void doing( String doing )
    {
        this.doing = doing;
    }
    
    public void done()
    {
        this.completedOperations.add( this.doing );
        this.doing = null;
    }
}
