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
package org.neo4j.kernel.impl.api.index;

import java.util.Iterator;

import org.neo4j.kernel.api.KernelAPI;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.transaction.xaframework.ForceMode;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Used to assert that Indexes required by Uniqueness Constraints don't remain if the constraint never got created.
 * Solves the case where the database crashes after the index for the constraint has been created but before the
 * constraint itself has been committed.
 */
public class RemoveOrphanConstraintIndexesOnStartup extends LifecycleAdapter
{
    private final AbstractTransactionManager txManager;
    private final KernelAPI kernel;

    public RemoveOrphanConstraintIndexesOnStartup( AbstractTransactionManager txManager, KernelAPI kernel )
    {
        this.txManager = txManager;
        this.kernel = kernel;
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    @Override
    public void start() throws Throwable
    {
        if ( txManager.getRecoveryError() != null )
        {
            // Recovery failed - bail out, we aren't going to be able to begin a transaction.
            // TODO: it seems wrong that we let all components start before we check if recovery failed...
            return;
        }
        txManager.begin( ForceMode.unforced );
        TransactionContext tx = kernel.newTransactionContext();
        boolean success = false;
        try
        {
            StatementContext context = tx.newStatementContext();
            try
            {
                for ( Iterator<IndexDescriptor> indexes = context.getIndexRules(); indexes.hasNext(); )
                {
                    IndexDescriptor index = indexes.next();
                    if ( index.isConstraintIndex() && context.getOwningConstraint( index ) == null )
                    {
                        context.dropIndexRule( index );
                    }
                }
            }
            finally
            {
                context.close();
            }
            success = true;
        }
        finally
        {
            if ( success )
            {
                tx.commit();
            }
            else
            {
                tx.rollback();
            }
        }
    }
}
