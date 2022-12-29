/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.state;

import java.util.Optional;

public interface TrialStatus {
    boolean expired();

    Optional<Long> daysLeft();

    static TrialStatus parse(String status) {
        if ("yes".equals(status)) return TrialStatusImpl.NOT_EXPIRED;
        if ("expired".equals(status)) return TrialStatusImpl.EXPIRED;
        if ("no".equals(status)) return TrialStatusImpl.NOT_EXPIRED;
        else return new TrialStatusImpl(false, Long.parseLong(status));
    }
}

class TrialStatusImpl implements TrialStatus {
    protected static TrialStatus NOT_EXPIRED = new TrialStatusImpl(false);
    protected static TrialStatus EXPIRED = new TrialStatusImpl(true);
    private final boolean expired;
    private final Long daysLeft;

    TrialStatusImpl(boolean expired) {
        this.expired = expired;
        this.daysLeft = null;
    }

    TrialStatusImpl(boolean expired, long daysLeft) {
        this.expired = expired;
        this.daysLeft = daysLeft;
    }

    @Override
    public boolean expired() {
        return expired;
    }

    @Override
    public Optional<Long> daysLeft() {
        return Optional.ofNullable(daysLeft);
    }
}
