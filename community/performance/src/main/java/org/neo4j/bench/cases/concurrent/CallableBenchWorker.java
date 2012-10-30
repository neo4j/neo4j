/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.bench.cases.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Callable;

public class CallableBenchWorker implements Callable<Collection<WorkerMetric>>
{

    private final BenchWorker worker;
    private final WorkerContext workerContext;

    public CallableBenchWorker( BenchWorker worker, WorkerContext
            workerContext )
    {

        this.worker = worker;
        this.workerContext = workerContext;
    }

    @Override
    public Collection<WorkerMetric> call() throws Exception
    {
        Collection<WorkerMetric> metrics = new ArrayList<WorkerMetric>();
        for ( WorkerMetric metric : worker.doWork( workerContext ) )
        {
            metrics.add( metric );
        }

        return metrics;
    }
}
