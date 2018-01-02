/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.stresstests.transaction.checkpoint.tracers;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.monitoring.tracing.TracerFactory;

public class TimerTracerFactory implements TracerFactory
{
    private TimerTransactionTracer timerTransactionTracer  = new TimerTransactionTracer();;

    @Override
    public String getImplementationName()
    {
        return "timer";
    }

    @Override
    public PageCacheTracer createPageCacheTracer()
    {
        return PageCacheTracer.NULL;
    }

    @Override
    public TransactionTracer createTransactionTracer()
    {
        return timerTransactionTracer;
    }

    @Override
    public CheckPointTracer createCheckPointTracer()
    {
        return timerTransactionTracer;
    }
}
