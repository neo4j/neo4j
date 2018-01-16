/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.state;

import org.neo4j.kernel.api.txstate.TransactionState;
import org.neo4j.kernel.api.txstate.TransactionStateController;

import static org.neo4j.unsafe.impl.internal.dragons.FeatureToggles.flag;

public class TransactionStatesContainer implements TransactionStateController
{
    private static final boolean MULTI_STATE = flag( TransactionStatesContainer.class, "multiState", false );
    private TransactionState globalTransactionState;
    private TxState stableTransactionState;

    public boolean hasChanges()
    {
        return hasState() && globalTransactionState.hasChanges();
    }

    public boolean hasState()
    {
        return globalTransactionState != null;
    }

    public boolean hasDataChanges()
    {
        return hasChanges() && globalTransactionState.hasDataChanges();
    }

    public TransactionState stable()
    {
        if ( !MULTI_STATE )
        {
            return global();
        }
        if ( stableTransactionState == null )
        {
            split();
        }
        return stableTransactionState;
    }

    public void validateForCommit()
    {
        if ( globalTransactionState instanceof CombinedTxState )
        {
            throw new IllegalStateException( "Commit of splitted transaction state is not supported." );
        }
    }

    public TransactionState global()
    {
        if ( globalTransactionState == null )
        {
            globalTransactionState = new TxState();
        }
        return globalTransactionState;
    }

    @Override
    public void combine()
    {
        if ( !MULTI_STATE )
        {
            return;
        }
        if ( globalTransactionState == null )
        {
            throw new IllegalStateException( "Can't combine state that was never splitted." );
        }
        if ( globalTransactionState instanceof CombinedTxState )
        {
            globalTransactionState = ((CombinedTxState) globalTransactionState).merge();
        }
        else
        {
            throw new IllegalStateException( "Merge should be performed only after transaction state split." );
        }
        stableTransactionState = null;
    }

    @Override
    public void split()
    {
        if ( !MULTI_STATE )
        {
            return;
        }
        stableTransactionState = (TxState) global();
        globalTransactionState = new CombinedTxState( stableTransactionState, new TxState() );
    }

    public void clear()
    {
        stableTransactionState = null;
        globalTransactionState = null;
    }
}
