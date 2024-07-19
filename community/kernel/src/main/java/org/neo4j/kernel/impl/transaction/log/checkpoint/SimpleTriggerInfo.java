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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.util.Objects;

/**
 * Simple implementation of a trigger info taking in construction the name/description of what triggered the check point
 * and offering the possibility to be enriched with a single optional extra description.
 */
public class SimpleTriggerInfo implements TriggerInfo {
    private final String triggerName;
    private String description;

    public SimpleTriggerInfo(String triggerName) {
        assert triggerName != null;
        this.triggerName = triggerName;
    }

    @Override
    public String describe(LatestCheckpointInfo checkpointInfo) {
        String info = description == null ? triggerName : triggerName + " for " + description;
        return "Checkpoint triggered by \"" + info + "\" @ txId: "
                + checkpointInfo.highestObservedClosedTransactionId().id() + ", append index: "
                + checkpointInfo.appendIndex();
    }

    @Override
    public void accept(String description) {
        assert description != null;
        assert this.description == null;
        this.description = description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleTriggerInfo that = (SimpleTriggerInfo) o;
        return Objects.equals(triggerName, that.triggerName) && Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(triggerName, description);
    }
}
