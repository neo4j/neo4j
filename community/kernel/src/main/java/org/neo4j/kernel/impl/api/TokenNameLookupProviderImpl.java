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

import org.neo4j.helpers.Function;
import org.neo4j.helpers.Function2;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.StatementTokenNameLookup;
import org.neo4j.kernel.api.operations.TokenNameLookup;
import org.neo4j.kernel.api.operations.TokenNameLookupProvider;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class TokenNameLookupProviderImpl implements TokenNameLookupProvider
{
    private final OldTxSafeStatementExecutor executor;

    public TokenNameLookupProviderImpl( OldTxSafeStatementExecutor executor )
    {
        this.executor = executor;
    }

    @Override
    public <T> T withTokenNameLookup( final Function<TokenNameLookup, T> work ) throws TransactionFailureException
    {
        return executor.executeSingleStatement( new Function2<StatementState, StatementOperationParts, T>()
        {
            @Override
            public T apply( StatementState state, StatementOperationParts logic )
            {
                return work.apply( new StatementTokenNameLookup( state, logic.keyReadOperations() ) );
            }
        } );
    }

    public static TokenNameLookupProviderImpl newForTransactionManager( AbstractTransactionManager txManager )
    {
        return new TokenNameLookupProviderImpl(
            new OldTxSafeStatementExecutor( "read-only statement for token name lookup", txManager ) );
    }
}
