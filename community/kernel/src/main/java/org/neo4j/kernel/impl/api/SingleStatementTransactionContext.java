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
package org.neo4j.kernel.impl.api;

import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.TransactionContext;

/**
 * Limits number of open statements to max 1, so that when starting another
 * statement the former is closed. {@link #finish() Finishing} the transaction
 * will also make sure any active statement is closed before doing so.
 */
public class SingleStatementTransactionContext extends DelegatingTransactionContext
{
    private InteractionStoppingStatementContext activeStatement;
    private boolean finished;

    public SingleStatementTransactionContext( TransactionContext delegate )
    {
        super( delegate );
    }

    @Override
    public StatementContext newStatementContext()
    {
        assertNewStatementAllowed();
        StatementContext inner = super.newStatementContext();
        activeStatement = new InteractionStoppingStatementContext( inner );
        return activeStatement;
    }

    @Override
    public void finish()
    {
        if ( anyActiveStatement() )
        {
            throw new IllegalStateException( "Cannot finish since there is an active statements" );
        }

        finished = true;
        super.finish();
    }

    private void assertNewStatementAllowed()
    {
        if ( finished )
        {
            throw new IllegalStateException( "This TransactionContext is finished. No new statements allowed" );
        }

        if ( anyActiveStatement() )
        {
            throw new IllegalStateException( "There is an active StatementContext. No new statements allowed" );
        }
    }

    private boolean anyActiveStatement()
    {
        return activeStatement != null && activeStatement.isOpen();
    }
}
