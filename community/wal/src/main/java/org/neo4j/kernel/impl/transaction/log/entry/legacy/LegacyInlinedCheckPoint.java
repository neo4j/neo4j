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
package org.neo4j.kernel.impl.transaction.log.entry.legacy;

import java.util.Objects;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.string.Mask;

public class LegacyInlinedCheckPoint extends AbstractLogEntry {
    private final LogPosition logPosition;

    LegacyInlinedCheckPoint(LogPosition logPosition) {
        super(LogEntryTypeCodes.LEGACY_CHECK_POINT);
        this.logPosition = logPosition;
    }

    public LogPosition getLogPosition() {
        return logPosition;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LegacyInlinedCheckPoint that = (LegacyInlinedCheckPoint) o;
        return Objects.equals(logPosition, that.logPosition);
    }

    @Override
    public int hashCode() {
        return Objects.hash(logPosition);
    }

    @Override
    public String toString(Mask mask) {
        return "LegacyInlinedCheckPoint{logPosition=" + logPosition + '}';
    }
}
