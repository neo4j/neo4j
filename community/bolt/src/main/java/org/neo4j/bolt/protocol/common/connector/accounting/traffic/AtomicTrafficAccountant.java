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
package org.neo4j.bolt.protocol.common.connector.accounting.traffic;

import java.time.Clock;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

public final class AtomicTrafficAccountant implements TrafficAccountant {

    private final long checkPeriodMillis;
    private final long readBandwidthThreshold;
    private final long writeBandwidthThreshold;
    private final long warningClearDuration;
    private final Clock clock;

    private final Log userLog;

    private final AtomicLong lastCheckMillis = new AtomicLong();

    private final AtomicLong bytesReadSinceLastCheck = new AtomicLong();
    private final AtomicLong bytesWrittenSinceLastCheck = new AtomicLong();

    private volatile long readThresholdLastExceededAt;
    private volatile long writeThresholdLastExceededAt;

    AtomicTrafficAccountant(
            long checkPeriodMillis,
            long readBandwidthThreshold,
            long writeBandwidthThreshold,
            long warningClearDuration,
            Clock clock,
            LogService logging) {
        this.checkPeriodMillis = checkPeriodMillis;
        this.readBandwidthThreshold = readBandwidthThreshold;
        this.writeBandwidthThreshold = writeBandwidthThreshold;
        this.warningClearDuration = warningClearDuration;
        this.clock = clock;

        this.userLog = logging.getUserLog(TrafficAccountant.class);
    }

    public AtomicTrafficAccountant(
            long checkPeriodMillis,
            long readBandwidthThreshold,
            long writeBandwidthThreshold,
            long warningClearDuration,
            LogService logging) {
        this(
                checkPeriodMillis,
                readBandwidthThreshold,
                writeBandwidthThreshold,
                warningClearDuration,
                Clock.systemUTC(),
                logging);
    }

    @Override
    public void notifyRead(long bytes) {
        this.bytesReadSinceLastCheck.addAndGet(bytes);
    }

    @Override
    public void notifyWrite(long bytes) {
        this.bytesWrittenSinceLastCheck.addAndGet(bytes);
    }

    @Override
    public void tryCheck() {
        // acquire the last time at which a check has been carried out and if this time exceeds the
        // configured period, attempt to swap it with our new value - if this succeeds, carry out the
        // check
        long lastCheckMillis, now;
        do {
            lastCheckMillis = this.lastCheckMillis.get();
            now = this.clock.millis();

            if (now - lastCheckMillis < this.checkPeriodMillis) {
                return;
            }
        } while (!this.lastCheckMillis.compareAndSet(lastCheckMillis, now));

        this.check(now, lastCheckMillis);
    }

    private void check(long now, long lastCheckMillis) {
        var windowDuration = now - lastCheckMillis;

        this.checkReadBytes(now, windowDuration);
        this.checkWrittenBytes(now, windowDuration);
    }

    private void checkReadBytes(long now, long windowDuration) {
        // always round bandwidth up to the nearest full number as this should always over report
        // slightly in order to provide early warnings
        var bytesSinceLastCheck = this.bytesReadSinceLastCheck.getAndSet(0);
        var bandwidth = bytesSinceLastCheck * 8.0 / 1_000_000 * (1_000.0 / windowDuration);
        if (bandwidth > this.readBandwidthThreshold) {
            if (this.readThresholdLastExceededAt == 0) {
                this.userLog.warn(
                        "Inbound bandwidth threshold has been exceeded (%.2f Mb/s exceeds configured threshold of %.2f Mb/s)",
                        bandwidth, (float) this.readBandwidthThreshold);
            }

            this.readThresholdLastExceededAt = now;
        } else if (this.readThresholdLastExceededAt != 0) {
            var millisSinceLastExceeded = now - this.readThresholdLastExceededAt;

            if (millisSinceLastExceeded >= this.warningClearDuration) {
                this.userLog.info(
                        "Inbound bandwidth has normalized (traffic has dropped below %.2f Mb/s for at least %d ms)",
                        (float) this.readBandwidthThreshold, this.warningClearDuration);
                this.readThresholdLastExceededAt = 0;
            }
        }
    }

    private void checkWrittenBytes(long now, long windowDuration) {
        // always round bandwidth up to the nearest full number as this should always over report
        // slightly in order to provide early warnings
        var bytesSinceLastCheck = this.bytesWrittenSinceLastCheck.getAndSet(0);
        var bandwidth = bytesSinceLastCheck * 8.0 / 1_000_000 * (1_000.0 / windowDuration);
        if (bandwidth > this.writeBandwidthThreshold) {
            if (this.writeThresholdLastExceededAt == 0) {
                this.userLog.warn(
                        "Outbound bandwidth threshold has been exceeded (%.2f Mb/s exceeds configured threshold of %.2f Mb/s)",
                        bandwidth, (float) this.writeBandwidthThreshold);
            }

            this.writeThresholdLastExceededAt = now;
        } else if (this.writeThresholdLastExceededAt != 0) {
            var millisSinceLastExceeded = now - this.writeThresholdLastExceededAt;

            if (millisSinceLastExceeded >= this.warningClearDuration) {
                this.userLog.info(
                        "Outbound bandwidth has normalized (traffic has dropped below %.2f Mb/s for at least %d ms)",
                        (float) this.writeBandwidthThreshold, this.warningClearDuration);
                this.writeThresholdLastExceededAt = 0;
            }
        }
    }
}
