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
package org.neo4j.dbms.admissioncontrol;

import java.time.Instant;
import org.neo4j.util.concurrent.BinaryLatch;

public final class AdmissionControlToken {
    public static final AdmissionControlToken ADMISSION_CONTROL_PROCESS_STOPPED =
            new AdmissionControlToken(AdmissionControlResponse.ADMISSION_CONTROL_PROCESS_STOPPED);
    public static final AdmissionControlToken UNABLE_TO_QUEUE_NEW_TOKEN =
            new AdmissionControlToken(AdmissionControlResponse.UNABLE_TO_ALLOCATE_NEW_TOKEN);
    public static final AdmissionControlToken RELEASED = new AdmissionControlToken(AdmissionControlResponse.RELEASED);

    private final BinaryLatch latch = new BinaryLatch();
    private final Instant queueTime;
    private volatile AdmissionControlResponse response;

    public AdmissionControlToken(Instant queueTime) {
        this.queueTime = queueTime;
    }

    private AdmissionControlToken(AdmissionControlResponse response) {
        latch.release();
        this.response = response;
        this.queueTime = null;
    }

    /**
     * The time at which this token was created, this is used by the admission control process to create metrics
     * regarding the queue times of successful admission control requests.
     * @return Time that this token was created.
     */
    public Instant queueTime() {
        return queueTime;
    }

    /**
     * For use by the admission control service, releases any underlying synchronisation mechanisms.
     * @param response The admission control service's recommended response to this request.
     */
    public void release(AdmissionControlResponse response) {
        this.response = response;
        latch.release();
    }

    /**
     * Awaits the release of the underlying control mechanism.
     * @return The response from the admission controller that created this token.
     */
    public AdmissionControlResponse await() {
        latch.await();
        return response;
    }
}
