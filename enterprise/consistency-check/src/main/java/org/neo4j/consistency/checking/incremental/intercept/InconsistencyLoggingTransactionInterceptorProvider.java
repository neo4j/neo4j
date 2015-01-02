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
import org.neo4j.consistency.checking.incremental.FullDiffCheck;
import org.neo4j.consistency.checking.incremental.IncrementalDiffCheck;
import org.neo4j.consistency.checking.incremental.LoggingDiffCheck;
import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;
import org.neo4j.kernel.impl.util.StringLogger;

@Service.Implementation(TransactionInterceptorProvider.class)
public class InconsistencyLoggingTransactionInterceptorProvider extends CheckingTransactionInterceptorProvider
{
    public static final String NAME = "inconsistency" + "log";

    public InconsistencyLoggingTransactionInterceptorProvider()
    {
        super( NAME );
    }

    public enum CheckerMode
    {
        FULL
        {
            @Override
            DiffCheck createChecker( StringLogger logger )
            {
                return new FullDiffCheck( logger );
            }
        },
        DIFF
        {
            @Override
            DiffCheck createChecker( StringLogger logger )
            {
                return new IncrementalDiffCheck( logger );
            }
        };

        abstract DiffCheck createChecker( StringLogger logger );
    }

    @Override
    DiffCheck createChecker( String mode, StringLogger logger )
    {
        final CheckerMode checkerMode;
        try
        {
            checkerMode = CheckerMode.valueOf( mode.toUpperCase() );
        }
        catch ( Exception e )
        {
            return null;
        }
        return new LoggingDiffCheck( checkerMode.createChecker( logger ), logger );
    }
}
