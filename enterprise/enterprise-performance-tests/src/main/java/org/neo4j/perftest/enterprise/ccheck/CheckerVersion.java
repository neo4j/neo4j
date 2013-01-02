/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.perftest.enterprise.ccheck;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.consistency.checking.full.FullCheck;
import org.neo4j.consistency.checking.old.ConsistencyRecordProcessor;
import org.neo4j.consistency.checking.old.ConsistencyReporter;
import org.neo4j.consistency.checking.old.InconsistencyType;
import org.neo4j.consistency.checking.old.MonitoringConsistencyReporter;
import org.neo4j.helpers.progress.ProgressMonitorFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.AbstractBaseRecord;
import org.neo4j.kernel.impl.nioneo.store.RecordStore;
import org.neo4j.kernel.impl.nioneo.store.StoreAccess;
import org.neo4j.kernel.impl.util.StringLogger;

enum CheckerVersion
{
    OLD
    {
        @Override
        void run( ProgressMonitorFactory progress, StoreAccess storeAccess, Config tuningConfiguration )
        {
            new ConsistencyRecordProcessor(
                    storeAccess,
                    new MonitoringConsistencyReporter( new ConsistencyReporter()
                    {
                        @Override
                        public <R1 extends AbstractBaseRecord, R2 extends AbstractBaseRecord> void report(
                                RecordStore<R1> recordStore,
                                R1 record,
                                RecordStore<? extends R2> referredStore,
                                R2 referred,
                                InconsistencyType inconsistency )
                        {
                        }

                        @Override
                        public <R extends AbstractBaseRecord> void report(
                                RecordStore<R> recordStore, R record,
                                InconsistencyType inconsistency )
                        {
                        }
                    } ), progress ).run();
        }
    },
    NEW
    {
        @Override
        void run( ProgressMonitorFactory progress, StoreAccess storeAccess, Config tuningConfiguration ) throws ConsistencyCheckIncompleteException
        {
            new FullCheck( tuningConfiguration, progress ).execute( storeAccess, StringLogger.DEV_NULL );
        }
    };

    abstract void run( ProgressMonitorFactory progress, StoreAccess storeAccess, Config tuningConfiguration )
            throws ConsistencyCheckIncompleteException;
}
