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
    private StatementContext activeStatement;
    private boolean finished;
    
    public SingleStatementTransactionContext( TransactionContext delegate )
    {
        super( delegate );
    }

    @Override
    public StatementContext newStatementContext()
    {
        assertNewStatementAllowed();
        closeAnyActiveStatement();
        StatementContext result = super.newStatementContext();
        // + Interaction stopping
        result = new InteractionStoppingStatementContext( result );
        activeStatement = result;
        return result;
    }

    @Override
    public void finish()
    {
        finished = true;
        closeAnyActiveStatement();
        super.finish();
    }

    private void closeAnyActiveStatement()
    {
        if ( activeStatement != null )
            activeStatement.close();
    }
    
    private void assertNewStatementAllowed()
    {
        if ( finished )
            throw new IllegalStateException( "This TransactionContext is finished. No new statements allowed" );
    }
}
