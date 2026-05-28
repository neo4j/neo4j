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
package org.neo4j.bolt.struct;

import java.io.IOException;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@IncludeWire(@Version(major = 4))
public class LegacyStructArgumentIT extends AbstractStructArgumentIT {

    @ProtocolTest
    void shouldFailWhenLegacyZonedDateTimeZoneIdIsNotKnown(@Authenticated BoltTestConnection connection)
            throws IOException {
        testFailureWithUnpackableValue(
                connection,
                buf -> buf.writeStructHeader(new StructHeader(3, StructType.DATE_TIME_ZONE_ID_LEGACY.getTag()))
                        .writeInt(0)
                        .writeInt(0)
                        .writeString("Europe/Marmaris"),
                "Illegal value for field \"params\": Illegal value for field \"tz_id\": Illegal zone identifier: \"Europe/Marmaris\"");
    }
}
