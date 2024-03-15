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
package org.neo4j.kernel.impl.store.record;

import java.util.Objects;
import org.neo4j.string.Mask;

public class MetaDataRecord extends AbstractBaseRecord {
    private long value;

    public MetaDataRecord() {
        super(-1);
    }

    public MetaDataRecord initialize(boolean inUse, long value) {
        super.initialize(inUse);
        this.value = value;
        return this;
    }

    @Override
    public void clear() {
        initialize(false, -1);
    }

    public long getValue() {
        return value;
    }

    @Override
    public String toString(Mask mask) {
        return String.format("Meta[%d,value:%d]", getId(), value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), value);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        MetaDataRecord other = (MetaDataRecord) obj;
        return value == other.value;
    }
}
