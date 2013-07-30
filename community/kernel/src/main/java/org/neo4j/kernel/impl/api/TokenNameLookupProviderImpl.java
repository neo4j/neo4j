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
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.kernel.api.StatementOperationParts;
import org.neo4j.kernel.api.exceptions.NoKernelException;
import org.neo4j.kernel.api.exceptions.TransactionalException;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.api.operations.StatementTokenNameLookup;
import org.neo4j.kernel.api.operations.TokenNameLookup;
import org.neo4j.kernel.api.operations.TokenNameLookupProvider;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;

public class TokenNameLookupProviderImpl implements TokenNameLookupProvider
{
    private final Transactor transactor;

    public TokenNameLookupProviderImpl( Transactor executor )
    {
        this.transactor = executor;
    }

    @Override
    public <T> T withTokenNameLookup( final Function<TokenNameLookup, T> work ) throws TransactionalException
    {
        try
        {
            return transactor.execute( new Transactor.Statement<T, NoKernelException>() {

                @Override
                public T perform( StatementOperationParts logic, StatementState state ) throws NoKernelException
                {
                    StatementTokenNameLookup lookup = new StatementTokenNameLookup( state, logic.keyReadOperations() );
                    return work.apply( lookup );
                }
            } );
        }
        catch ( NoKernelException e )
        {
            throw new ThisShouldNotHappenError( "Stefan", "NoKernelException is not expected to EVER be instantiated" );
        }
    }

    public static TokenNameLookupProviderImpl newForTransactionManager( AbstractTransactionManager txManager )
    {
        return new TokenNameLookupProviderImpl( new Transactor( txManager ) );
    }
}
