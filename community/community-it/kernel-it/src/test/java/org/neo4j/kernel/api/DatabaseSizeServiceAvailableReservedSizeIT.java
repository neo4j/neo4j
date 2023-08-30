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
package org.neo4j.kernel.api;

import org.junit.jupiter.api.Nested;
import org.neo4j.kernel.impl.store.format.aligned.PageAligned;
import org.neo4j.kernel.impl.store.format.standard.Standard;

class DatabaseSizeServiceAvailableReservedSizeIT {

    @Nested
    class RecordFormatStandard extends RecordFormatDatabaseSizeServiceAvailableReservedSizeIT {
        RecordFormatStandard() {
            super(Standard.LATEST_RECORD_FORMATS);
        }
    }

    @Nested
    class RecordFormatAligned extends RecordFormatDatabaseSizeServiceAvailableReservedSizeIT {
        RecordFormatAligned() {
            super(PageAligned.LATEST_RECORD_FORMATS);
        }
    }
}
