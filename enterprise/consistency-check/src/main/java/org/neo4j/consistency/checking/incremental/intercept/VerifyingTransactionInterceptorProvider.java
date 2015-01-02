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
package org.neo4j.consistency.checking.incremental.intercept;

import org.neo4j.consistency.checking.incremental.DiffCheck;
import org.neo4j.consistency.checking.incremental.IncrementalDiffCheck;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation(TransactionInterceptorProvider.class)
public class VerifyingTransactionInterceptorProvider extends CheckingTransactionInterceptorProvider
{
    public static final String NAME = "verifying";

    public VerifyingTransactionInterceptorProvider()
    {
        super( NAME );
    }

    @Override
    DiffCheck createChecker( String mode, StringLogger logger )
    {
        if ( "true".equalsIgnoreCase( mode ) )
        {
            return new IncrementalDiffCheck( logger );
        }
        return null;
    }
}
