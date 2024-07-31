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
package org.neo4j.bolt.protocol.v40.fsm.response.metadata;

import org.neo4j.bolt.protocol.common.fsm.response.metadata.AbstractLegacyMetadataHandlerTest;
import org.neo4j.bolt.protocol.common.fsm.response.metadata.MetadataHandler;
import org.neo4j.bolt.testing.assertions.MapValueAssertions;
import org.neo4j.values.virtual.MapValue;

public class MetadataHandlerV40Test extends AbstractLegacyMetadataHandlerTest {

    @Override
    protected MetadataHandler createMetadataHandler() {
        return MetadataHandlerV40.getInstance();
    }

    @Override
    protected void verifyApplyUpdateQueryStatisticsResult(MapValue value) {
        super.verifyApplyUpdateQueryStatisticsResult(value);

        MapValueAssertions.assertThat(value)
                // system updates and regular updates don't mix
                .hasSize(11);
    }

    @Override
    protected void verifyApplySystemQueryStatisticsResult(MapValue value) {
        super.verifyApplySystemQueryStatisticsResult(value);

        MapValueAssertions.assertThat(value)
                // system updates and regular updates don't mix
                .hasSize(1);
    }
}
