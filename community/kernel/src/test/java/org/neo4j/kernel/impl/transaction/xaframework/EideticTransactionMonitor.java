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
package org.neo4j.kernel.impl.transaction.xaframework;

import javax.transaction.xa.Xid;

public class EideticTransactionMonitor implements TransactionMonitor
{
    private int commitCount;
    private int injectOnePhaseCommitCount;
    private int injectTwoPhaseCommitCount;

    @Override
    public void transactionCommitted( Xid xid, boolean recovered )
    {
        commitCount++;
    }

    @Override
    public void injectOnePhaseCommit( Xid xid )
    {
        injectOnePhaseCommitCount++;
    }

    @Override
    public void injectTwoPhaseCommit( Xid xid )
    {
        injectTwoPhaseCommitCount++;
    }

    public int getCommitCount()
    {
        return commitCount;
    }

    public int getInjectOnePhaseCommitCount()
    {
        return injectOnePhaseCommitCount;
    }

    public int getInjectTwoPhaseCommitCount()
    {
        return injectTwoPhaseCommitCount;
    }
}
