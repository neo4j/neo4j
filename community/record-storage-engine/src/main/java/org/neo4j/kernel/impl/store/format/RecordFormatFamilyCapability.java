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
package org.neo4j.kernel.impl.store.format;

import java.util.Objects;
import org.neo4j.storageengine.api.format.Capability;
import org.neo4j.storageengine.api.format.CapabilityType;

public class RecordFormatFamilyCapability implements Capability {
    private final FormatFamily family;

    public RecordFormatFamilyCapability(FormatFamily family) {
        this.family = family;
    }

    @Override
    public boolean isType(CapabilityType type) {
        return type == CapabilityType.FORMAT;
    }

    @Override
    public boolean isAdditive() {
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RecordFormatFamilyCapability that = (RecordFormatFamilyCapability) o;
        return family.equals(that.family);
    }

    @Override
    public int hashCode() {
        return Objects.hash(family);
    }

    @Override
    public String toString() {
        return "RecordFormatFamilyCapability{" + "family='" + family + '\'' + '}';
    }
}
