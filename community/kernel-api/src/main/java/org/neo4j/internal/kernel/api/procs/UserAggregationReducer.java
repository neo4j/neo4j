/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.kernel.api.procs;

import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.values.AnyValue;

/**
 * We will only create a single reduce but when called from parallel runtime it can in turn span many updaters.
 *
 * <p>Example usage: A simple sum-aggregator:
 *
 * <pre>{@code
 *   class SumReducer implements UserAggregationReducer {
 *         private final LongAdder globalSum = new LongAdder();
 *
 *         @Override
 *         public UserAggregationUpdater newUpdater() throws ProcedureException {
 *             return new SumUpdater();
 *         }
 *
 *         @Override
 *         public AnyValue result() throws ProcedureException {
 *             return Values.longValue(globalSum.sum());
 *         }
 *
 *         class SumUpdater implements UserAggregationUpdater {
 *             private long localSum;
 *
 *             @Override
 *             public void update(AnyValue[] input) throws ProcedureException {
 *                 if (input[0] instanceof NumberValue value) {
 *                     localSum += value.longValue();
 *                 }
 *             }
 *
 *             @Override
 *             public void applyUpdates() throws ProcedureException {
 *                 globalSum.addAndGet(localSum);
 *                 localSum = 0;
 *             }
 *         }
 *     }
 * }</pre>
 */
public interface UserAggregationReducer {

    /**
     * @return an updater responsible for updating and report back the result of the aggregation
     */
    UserAggregationUpdater newUpdater() throws ProcedureException;

    /**
     * @return the result of the aggregation
     */
    AnyValue result() throws ProcedureException;
}
