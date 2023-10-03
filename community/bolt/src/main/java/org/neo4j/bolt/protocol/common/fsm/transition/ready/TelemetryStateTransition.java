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

package org.neo4j.bolt.protocol.common.fsm.transition.ready;

import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.AbstractStateTransition;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.generic.TelemetryMessage;

public class TelemetryStateTransition extends AbstractStateTransition<TelemetryMessage> {
    private static final TelemetryStateTransition INSTANCE = new TelemetryStateTransition();

    private TelemetryStateTransition() {
        super(TelemetryMessage.class);
    }

    public static TelemetryStateTransition getInstance() {
        return INSTANCE;
    }

    @Override
    public StateReference process(Context ctx, TelemetryMessage message, ResponseHandler handler) {

        var metricsMonitor = ctx.connection().connector().driverMetricsMonitor();

        // Will be noop if telemetry is disabled on the server, but we want to continue as normal regardless
        switch (message.interfaceType()) {
            case EXECUTE_QUERY -> metricsMonitor.executeInterfaceCalled();
            case MANAGED_TRANSACTION -> metricsMonitor.managedTransactionFunctionsInterfaceCalled();
            case UNMANAGED_TRANSACTION -> metricsMonitor.unmanagedTransactionInterfaceCalled();
            case TRANSACTION_FUNCTION -> metricsMonitor.implicitTransactionInterfaceCalled();
        }

        return ctx.state();
    }
}
