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
package org.neo4j.shell.state;

import java.util.Optional;

public interface LicenseDetails {
    enum Status {
        YES,
        NO,
        EVAL,
        EXPIRED
    }

    Status status();

    Optional<Long> daysLeft();

    Optional<Long> trialDays();

    static LicenseDetails parse(String status, long daysLeftOnTrial, long totalTrialDays) {
        if ("yes".equals(status)) {
            return LicenseDetailsImpl.YES;
        }
        if ("no".equals(status)) {
            return LicenseDetailsImpl.NO;
        }
        if ("expired".equals(status)) {
            return new LicenseDetailsImpl(Status.EXPIRED, Optional.of(0L), Optional.of(totalTrialDays));
        }
        if ("eval".equals(status)) {
            return new LicenseDetailsImpl(Status.EVAL, Optional.of(daysLeftOnTrial), Optional.of(totalTrialDays));
        }
        throw new IllegalArgumentException("invalid license status " + status);
    }
}

record LicenseDetailsImpl(Status status, Optional<Long> daysLeft, Optional<Long> trialDays) implements LicenseDetails {
    static final LicenseDetails YES = new LicenseDetailsImpl(Status.YES, Optional.empty(), Optional.empty());
    static final LicenseDetails NO = new LicenseDetailsImpl(Status.NO, Optional.empty(), Optional.empty());
}
