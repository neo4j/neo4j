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
package org.neo4j.bolt.protocol.common.fsm.transition;

import java.time.Clock;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import org.neo4j.bolt.fsm.Context;
import org.neo4j.bolt.fsm.state.StateReference;
import org.neo4j.bolt.fsm.state.transition.StateTransition;
import org.neo4j.bolt.protocol.common.connector.connection.ConnectionHandle;
import org.neo4j.bolt.protocol.common.fsm.States;
import org.neo4j.bolt.protocol.common.fsm.response.ResponseHandler;
import org.neo4j.bolt.protocol.common.message.request.RequestMessage;
import org.neo4j.bolt.testing.mock.ConnectionMockFactory;

public abstract class AbstractStateTransitionTest<R extends RequestMessage, T extends StateTransition<R>> {

    protected Context context;
    protected ConnectionHandle connection;
    protected Clock clock;

    protected T transition;
    protected ResponseHandler responseHandler;

    @BeforeEach
    protected void prepareContext() throws Exception {
        this.context = Mockito.mock(Context.class);
        this.connection = ConnectionMockFactory.newInstance();
        this.clock = Mockito.mock(Clock.class);
        this.responseHandler = Mockito.mock(ResponseHandler.class);

        Mockito.doReturn(this.connection).when(this.context).connection();
        Mockito.doReturn(this.clock).when(this.context).clock();
        Mockito.doReturn(this.initialState()).when(this.context).state();

        Mockito.doReturn(0L, 42L, 84L).when(this.clock).millis();
        Mockito.doReturn(Instant.EPOCH, Instant.ofEpochMilli(42), Instant.ofEpochMilli(84))
                .when(this.clock)
                .instant();

        this.transition = this.getTransition();
    }

    protected StateReference initialState() {
        return States.READY;
    }

    protected abstract T getTransition();
}
