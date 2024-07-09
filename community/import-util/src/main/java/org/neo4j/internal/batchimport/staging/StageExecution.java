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
package org.neo4j.internal.batchimport.staging;

import static org.neo4j.internal.helpers.Exceptions.throwIfUnchecked;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.executor.ProcessorScheduler;
import org.neo4j.internal.batchimport.stats.Key;
import org.neo4j.internal.batchimport.stats.Stat;

/**
 * Default implementation of {@link StageControl}
 */
public class StageExecution implements StageControl, AutoCloseable {
    public static final PanicMonitor DEFAULT_PANIC_MONITOR =
            cause -> System.err.println("Critical error occurred! Shutting down the import...");

    private final String stageName;
    private final String part;
    private final Configuration config;
    private final Collection<Step<?>> pipeline;
    private final int orderingGuarantees;
    private volatile Throwable panic;
    private final boolean shouldRecycle;
    private final ProcessorScheduler scheduler;
    private final PanicMonitor panicMonitor;
    private final ConcurrentLinkedQueue<Object> recycled;

    public StageExecution(
            String stageName, String part, Configuration config, Collection<Step<?>> pipeline, int orderingGuarantees) {
        this(
                stageName,
                part,
                config,
                pipeline,
                orderingGuarantees,
                ProcessorScheduler.SPAWN_THREAD,
                DEFAULT_PANIC_MONITOR);
    }

    public StageExecution(
            String stageName,
            String part,
            Configuration config,
            Collection<Step<?>> pipeline,
            int orderingGuarantees,
            ProcessorScheduler scheduler,
            PanicMonitor panicMonitor) {
        this.stageName = stageName;
        this.part = part;
        this.config = config;
        this.pipeline = pipeline;
        this.orderingGuarantees = orderingGuarantees;
        this.shouldRecycle = (orderingGuarantees & Step.RECYCLE_BATCHES) != 0;
        this.scheduler = scheduler;
        this.panicMonitor = panicMonitor;
        this.recycled = shouldRecycle ? new ConcurrentLinkedQueue<>() : null;
    }

    public boolean stillExecuting() {
        for (Step<?> step : pipeline) {
            if (!step.isCompleted()) {
                return true;
            }
        }
        return false;
    }

    public void awaitCompletion() throws InterruptedException {
        awaitCompletion(Long.MAX_VALUE, TimeUnit.HOURS);
    }

    public boolean awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        for (Step<?> step : pipeline) {
            if (!step.awaitCompleted(timeout, unit)) {
                return false;
            }
        }
        return true;
    }

    public void start() {
        for (Step<?> step : pipeline) {
            step.start(orderingGuarantees);
        }
    }

    public String getStageName() {
        return stageName;
    }

    public String name() {
        return stageName + (part != null ? part : "");
    }

    public Configuration getConfig() {
        return config;
    }

    public Iterable<Step<?>> steps() {
        return pipeline;
    }

    /**
     * @param stat statistics {@link Key}.
     * @param trueForAscending {@code true} for ordering by ascending, otherwise descending.
     * @return the steps ordered by the {@link Stat#asLong() long value representation} of the given
     * {@code stat} accompanied a factor by how it compares to the next value, where a value close to
     * {@code 1.0} signals them being close to equal, and a value of for example {@code 0.5} signals that
     * the value of the current step is half that of the next step.
     */
    public List<WeightedStep> stepsOrderedBy(final Key stat, final boolean trueForAscending) {
        final List<Step<?>> steps = new ArrayList<>(pipeline);
        steps.sort((o1, o2) -> {
            long stat1 = o1.longStat(stat);
            long stat2 = o2.longStat(stat);
            return trueForAscending ? Long.compare(stat1, stat2) : Long.compare(stat2, stat1);
        });
        List<WeightedStep> result = new ArrayList<>();
        for (int i = 0, numSteps = steps.size(); i < numSteps - 1; i++) {
            Step<?> current = steps.get(i);
            result.add(new WeightedStep(
                    current,
                    (float) current.longStat(stat) / (float) steps.get(i + 1).longStat(stat)));
        }
        result.add(new WeightedStep(steps.get(steps.size() - 1), 1.0f));
        return result;
    }

    public int size() {
        return pipeline.size();
    }

    @Override
    public synchronized void panic(Throwable cause) {
        if (panic == null) {
            panicMonitor.receivedPanic(cause);
            panic = cause;
            for (Step<?> step : pipeline) {
                step.receivePanic(cause);
                step.endOfUpstream();
            }
        } else {
            if (!panic.equals(cause)) {
                panic.addSuppressed(cause);
            }
        }
    }

    @Override
    public void assertHealthy() {
        if (panic != null) {
            throwIfUnchecked(panic);
            throw new RuntimeException(panic);
        }
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[" + name() + "]";
    }

    @Override
    public void recycle(Object batch) {
        if (shouldRecycle) {
            recycled.offer(batch);
        }
    }

    @Override
    public <T> T reuse(Supplier<T> fallback) {
        if (shouldRecycle) {
            @SuppressWarnings("unchecked")
            T result = (T) recycled.poll();
            if (result != null) {
                return result;
            }
        }

        return fallback.get();
    }

    @Override
    public boolean isIdle() {
        int i = 0;
        for (Step<?> step : steps()) {
            if (i++ > 0) {
                if (!step.isIdle()) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public ProcessorScheduler scheduler() {
        return scheduler;
    }

    @Override
    public void close() {
        if (shouldRecycle) {
            recycled.clear();
        }
    }

    @FunctionalInterface
    interface PanicMonitor {
        void receivedPanic(Throwable cause);
    }
}
