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
package org.neo4j.io.pagecache.tracing;

import java.util.concurrent.atomic.LongAdder;

public class DefaultPageFileSwapperTracer implements PageFileSwapperTracer {
    private final LongAdder pins = new LongAdder();
    private final LongAdder unpins = new LongAdder();
    private final LongAdder hits = new LongAdder();
    private final LongAdder faults = new LongAdder();
    private final LongAdder failedFaults = new LongAdder();
    private final LongAdder noFaults = new LongAdder();
    private final LongAdder vectoredFaults = new LongAdder();
    private final LongAdder failedVectoredFaults = new LongAdder();
    private final LongAdder noPinFaults = new LongAdder();
    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder bytesWritten = new LongAdder();
    private final LongAdder evictions = new LongAdder();
    private final LongAdder evictionExceptions = new LongAdder();
    private final LongAdder flushes = new LongAdder();
    private final LongAdder merges = new LongAdder();

    /**
     * Report number of observed pins
     * @param pins number of pins
     */
    @Override
    public void pins(long pins) {
        this.pins.add(pins);
    }

    /**
     * Report number of observed unpins
     * @param unpins number of unpins
     */
    @Override
    public void unpins(long unpins) {
        this.unpins.add(unpins);
    }

    /**
     * Report number of observer hits
     * @param hits number of hits
     */
    @Override
    public void hits(long hits) {
        this.hits.add(hits);
    }

    /**
     * Report number of observed faults
     * @param faults number of faults
     */
    @Override
    public void faults(long faults) {
        this.faults.add(faults);
    }

    @Override
    public void noFaults(long noFaults) {
        this.noFaults.add(noFaults);
    }

    @Override
    public void failedFaults(long failedFaults) {
        this.failedFaults.add(failedFaults);
    }

    @Override
    public void vectoredFaults(long faults) {
        this.vectoredFaults.add(faults);
    }

    @Override
    public void failedVectoredFaults(long failedFaults) {
        this.failedVectoredFaults.add(failedFaults);
    }

    @Override
    public void noPinFaults(long faults) {
        this.noPinFaults.add(faults);
    }

    /**
     * Report number of bytes read
     * @param bytesRead number of read bytes
     */
    @Override
    public void bytesRead(long bytesRead) {
        this.bytesRead.add(bytesRead);
    }

    /**
     * Report number of observed evictions
     * @param evictions number of evictions
     */
    @Override
    public void evictions(long evictions) {
        this.evictions.add(evictions);
    }

    /**
     * Report number of eviction exceptions
     * @param evictionExceptions number of eviction exceptions
     */
    @Override
    public void evictionExceptions(long evictionExceptions) {
        this.evictionExceptions.add(evictionExceptions);
    }

    /**
     * Report number of bytes written
     * @param bytesWritten number of written bytes
     */
    @Override
    public void bytesWritten(long bytesWritten) {
        this.bytesWritten.add(bytesWritten);
    }

    /**
     * Report number of flushes
     * @param flushes number of flushes
     */
    @Override
    public void flushes(long flushes) {
        this.flushes.add(flushes);
    }

    /**
     * Report number of merges
     * @param merges number of merges
     */
    @Override
    public void merges(long merges) {
        this.merges.add(merges);
    }

    @Override
    public long faults() {
        return faults.sum();
    }

    @Override
    public long failedFaults() {
        return failedFaults.sum();
    }

    @Override
    public long noFaults() {
        return noFaults.sum();
    }

    @Override
    public long vectoredFaults() {
        return vectoredFaults.sum();
    }

    @Override
    public long failedVectoredFaults() {
        return failedVectoredFaults.sum();
    }

    @Override
    public long noPinFaults() {
        return noPinFaults.sum();
    }

    @Override
    public long evictions() {
        return evictions.sum();
    }

    @Override
    public long pins() {
        return pins.sum();
    }

    @Override
    public long unpins() {
        return unpins.sum();
    }

    @Override
    public long hits() {
        return hits.sum();
    }

    @Override
    public long flushes() {
        return flushes.sum();
    }

    @Override
    public long merges() {
        return merges.sum();
    }

    @Override
    public long bytesRead() {
        return bytesRead.sum();
    }

    @Override
    public long bytesWritten() {
        return bytesWritten.sum();
    }

    @Override
    public long evictionExceptions() {
        return evictionExceptions.sum();
    }
}
