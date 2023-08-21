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
package org.neo4j.bolt.testing.fsm;

import java.util.stream.Stream;
import org.neo4j.bolt.negotiation.ProtocolVersion;
import org.neo4j.bolt.protocol.common.BoltProtocol;
import org.neo4j.bolt.testing.messages.BoltMessages;

public interface StateMachineProvider {

    static Stream<StateMachineProvider> versions() {
        return Stream.of(
                StateMachineV40Provider.getInstance(),
                StateMachineV41Provider.getInstance(),
                StateMachineV42Provider.getInstance(),
                StateMachineV43Provider.getInstance(),
                StateMachineV44Provider.getInstance(),
                StateMachineV50Provider.getInstance(),
                StateMachineV51Provider.getInstance(),
                StateMachineV52Provider.getInstance());
    }

    default ProtocolVersion version() {
        return this.messages().version();
    }

    BoltMessages messages();

    BoltProtocol protocol();
}
