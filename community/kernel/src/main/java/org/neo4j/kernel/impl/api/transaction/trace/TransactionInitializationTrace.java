/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.impl.api.transaction.trace;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

public class TransactionInitializationTrace
{
    private static final String MESSAGE = "Transaction initialization stacktrace.";
    public static final TransactionInitializationTrace NONE = new EmptyTransactionInitializationTrace();

    private final Throwable trace;

    public TransactionInitializationTrace()
    {
        trace = new Throwable( MESSAGE );
    }

    public String getTrace()
    {
        return ExceptionUtils.getStackTrace( trace );
    }

    private static class EmptyTransactionInitializationTrace extends TransactionInitializationTrace
    {
        @Override
        public String getTrace()
        {
            return StringUtils.EMPTY;
        }
    }
}
