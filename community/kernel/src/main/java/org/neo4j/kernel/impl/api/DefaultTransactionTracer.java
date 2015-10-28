/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;

public class DefaultTransactionTracer implements TransactionTracer, LogRotationMonitor
{
    private final Clock clock;

    private volatile long startTime = -1;
    private volatile long endTime = -1;
    private volatile long totalTime = -1;

    private final LogRotateEvent logRotateEvent = new LogRotateEvent()
    {
        @Override
        public void close()
        {
            endTime = clock.currentTimeMillis();
            totalTime = endTime - startTime;
        }
    };

    private final LogAppendEvent logAppendEvent = new LogAppendEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public void setLogRotated( boolean logRotated )
        {

        }

        @Override
        public LogRotateEvent beginLogRotate()
        {
            startTime = clock.currentTimeMillis();
            return logRotateEvent;
        }

        @Override
        public SerializeTransactionEvent beginSerializeTransaction()
        {
            return SerializeTransactionEvent.NULL;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait()
        {
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            return LogForceEvent.NULL;
        }
    };

    private final CommitEvent commitEvent = new CommitEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public LogAppendEvent beginLogAppend()
        {
            return logAppendEvent;
        }

        @Override
        public StoreApplyEvent beginStoreApply()
        {
            return StoreApplyEvent.NULL;
        }

        @Override
        public void setTransactionId( long transactionId )
        {
        }
    };

    private final TransactionEvent transactionEvent = new TransactionEvent()
    {

        @Override
        public void setSuccess( boolean success )
        {
        }

        @Override
        public void setFailure( boolean failure )
        {
        }

        @Override
        public CommitEvent beginCommitEvent()
        {
            return commitEvent;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void setTransactionType( String transactionTypeName )
        {
        }

        @Override
        public void setReadOnly( boolean wasReadOnly )
        {
        }
    };

    public DefaultTransactionTracer()
    {
        this( Clock.SYSTEM_CLOCK );
    }

    public DefaultTransactionTracer( Clock clock )
    {
        this.clock = clock;
    }

    @Override
    public TransactionEvent beginTransaction()
    {
        return transactionEvent;
    }

    @Override
    public long lastLogRotationStartTime()
    {
        return startTime;
    }

    @Override
    public long lastLogRotationEndTime()
    {
        return endTime;
    }

    @Override
    public long lastLogRotationTotalTime()
    {
        return totalTime;
    }
}
