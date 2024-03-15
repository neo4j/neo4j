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

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Objects;
import org.neo4j.string.Mask;

public class SchemaRecord extends PrimitiveRecord {
    public static final long SHALLOW_SIZE = shallowSizeOfInstance(SchemaRecord.class);
    public static final byte COMMAND_HAS_NO_SCHEMA_RULE = 0;
    public static final byte COMMAND_HAS_SCHEMA_RULE = 1;
    public static final byte SCHEMA_FLAG_IS_CONSTRAINT = 1;

    private boolean constraint;

    public SchemaRecord(long id) {
        super(id);
    }

    public SchemaRecord(SchemaRecord other) {
        super(other);
        this.constraint = other.constraint;
    }

    @Override
    public void setIdTo(PropertyRecord property) {
        property.setSchemaRuleId(getId());
    }

    @Override
    public SchemaRecord initialize(boolean inUse, long nextProp) {
        super.initialize(inUse, nextProp);
        return this;
    }

    @Override
    public String toString(Mask mask) {
        return "SchemaRecord[" + getId() + ",used=" + inUse() + ",created=" + isCreated() + ",nextProp=" + nextProp
                + ",constraint=" + constraint + ",secondaryUnitId" + getSecondaryUnitId() + ",fixedReferences="
                + isUseFixedReferences() + "]";
    }

    public boolean isConstraint() {
        return constraint;
    }

    public void setConstraint(boolean constraint) {
        this.constraint = constraint;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), constraint);
    }

    @Override
    public boolean equals(Object obj) {
        if (!super.equals(obj)) {
            return false;
        }
        SchemaRecord other = (SchemaRecord) obj;
        return constraint == other.constraint;
    }
}
