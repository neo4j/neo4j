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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.helpers.collection.Iterables.asList;
import static org.neo4j.io.IOUtils.closeAllSilently;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.stats.Keys;
import org.neo4j.internal.batchimport.stats.StepStats;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;

class ForkedProcessorStepTest {
    private static final int TIMEOUT_MINUTES = 2;
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Test
    void shouldProcessAllSingleThreaded() throws Exception {
        // GIVEN
        StageControl control = mock(StageControl.class);
        int maxProcessors = 10;

        int batches = 10;
        BatchProcessor step = new BatchProcessor(control, maxProcessors);
        TrackingStep downstream = new TrackingStep();
        step.setDownstream(downstream);
        int processors = step.processors(maxProcessors - step.processors(0));

        // WHEN
        step.start(0);
        for (int i = 1; i <= batches; i++) {
            step.receive(i, new Batch(processors));
        }
        step.endOfUpstream();
        step.awaitCompleted();
        downstream.close();
        step.close();

        // THEN
        assertEquals(batches, downstream.received.get());
    }

    @Test
    @Timeout(value = TIMEOUT_MINUTES, unit = MINUTES)
    void shouldProcessAllBatchesOnSingleCoreSystems() throws Exception {
        // GIVEN
        StageControl control = mock(StageControl.class);
        int processors = 1;

        int batches = 10;
        BatchProcessor step = new BatchProcessor(control, processors);
        TrackingStep downstream = new TrackingStep();
        step.setDownstream(downstream);

        // WHEN
        step.start(0);
        for (int i = 1; i <= batches; i++) {
            step.receive(i, new Batch(processors));
        }
        step.endOfUpstream();
        step.awaitCompleted();
        step.close();

        // THEN
        assertEquals(batches, downstream.received.get());
    }

    @Test
    void mustNotDetachProcessorsFromBatchChains() throws Exception {
        // GIVEN
        StageControl control = mock(StageControl.class);
        int processors = 1;

        int batches = 10;
        BatchProcessor step = new BatchProcessor(control, processors);
        TrackingStep downstream = new TrackingStep();
        step.setDownstream(downstream);
        int delta = processors - step.processors(0);
        step.processors(delta);

        // WHEN
        step.start(0);
        for (int i = 1; i <= batches; i++) {
            step.receive(i, new Batch(processors));
        }
        step.endOfUpstream();
        step.awaitCompleted();
        step.close();

        // THEN
        assertEquals(batches, downstream.received.get());
    }

    @Test
    void shouldProcessAllMultiThreadedAndWithChangingProcessorCount() throws Exception {
        // GIVEN
        StageControl control = mock(StageControl.class);
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        BatchProcessor step = new BatchProcessor(control, availableProcessors);
        TrackingStep downstream = new TrackingStep();
        step.setDownstream(downstream);

        // WHEN
        step.start(0);
        AtomicLong nextTicket = new AtomicLong();
        Thread[] submitters = new Thread[3];
        AtomicBoolean end = new AtomicBoolean();
        for (int i = 0; i < submitters.length; i++) {
            submitters[i] = new Thread(() -> {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                while (!end.get()) {
                    // synchronized block simulating that batches are received in order,
                    // which is enforced in real environment of a stage.
                    synchronized (nextTicket) {
                        // The processor count is changed here in this block simply because otherwise
                        // it's very hard to know how many processors we expect to see have processed
                        // a particular batch.
                        if (random.nextFloat() < 0.1) {
                            step.processors(random.nextInt(-2, 4));
                        }

                        long ticket = nextTicket.incrementAndGet();
                        Batch batch = new Batch(step.processors(0));
                        step.receive(ticket, batch);
                    }
                }
            });
            submitters[i].start();
        }

        while (downstream.received.get() < 200) {
            Thread.sleep(10);
        }
        end.set(true);
        for (Thread submitter : submitters) {
            submitter.join();
        }
        step.endOfUpstream();
        step.close();
    }

    @Test
    void shouldKeepForkedOrderIntactWhenChangingProcessorCount() throws Exception {
        int length = 100;
        AtomicIntegerArray reference = new AtomicIntegerArray(length);

        // GIVEN
        StageControl control = new SimpleStageControl();
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        ForkedProcessorStep<int[]> step = new ForkedProcessorStep<>(control, "Processor", config(availableProcessors)) {
            @Override
            protected void forkedProcess(int id, int processors, int[] batch) throws InterruptedException {
                int ticket = batch[0];
                Thread.sleep(ThreadLocalRandom.current().nextInt(10));
                for (int i = 1; i < batch.length; i++) {
                    if (batch[i] % processors == id) {
                        boolean compareAndSet = reference.compareAndSet(batch[i], ticket, ticket + 1);
                        assertTrue(
                                compareAndSet,
                                "I am " + id + ". Was expecting " + ticket + " for " + batch[i] + " but was "
                                        + reference.get(batch[i]));
                    }
                }
            }
        };
        DeadEndStep downstream = new DeadEndStep(control, CONTEXT_FACTORY);
        step.setDownstream(downstream);

        // WHEN
        step.start(0);
        downstream.start(0);
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int ticket = 0; ticket < 200; ticket++) {
            // The processor count is changed here in this block simply because otherwise
            // it's very hard to know how many processors we expect to see have processed
            // a particular batch.
            if (random.nextFloat() < 0.1) {
                step.processors(random.nextInt(-2, 4));
            }

            int[] batch = new int[length];
            batch[0] = ticket;
            for (int j = 1; j < batch.length; j++) {
                batch[j] = j - 1;
            }
            step.receive(ticket, batch);
        }
        step.endOfUpstream();
        step.awaitCompleted();
        step.close();
        downstream.close();
    }

    @Test
    void shouldPanicOnFailure() throws Exception {
        // GIVEN
        SimpleStageControl control = new SimpleStageControl();
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        Exception testPanic = new RuntimeException();
        ForkedProcessorStep<Void> step = new ForkedProcessorStep<>(control, "Processor", config(availableProcessors)) {
            @Override
            protected void forkedProcess(int id, int processors, Void batch) throws Throwable {
                throw testPanic;
            }
        };

        // WHEN
        control.steps(step);
        step.start(0);
        step.receive(1, null);

        // THEN
        step.awaitCompleted();
        try {
            control.assertHealthy();
        } catch (Exception e) {
            assertSame(testPanic, e);
        }
    }

    @Test
    @Timeout(value = TIMEOUT_MINUTES, unit = MINUTES)
    void shouldBeAbleToProgressUnderStressfulProcessorChangesWhenOrdered() throws Exception {
        shouldBeAbleToProgressUnderStressfulProcessorChanges(Step.ORDER_SEND_DOWNSTREAM);
    }

    @Test
    @Timeout(value = TIMEOUT_MINUTES, unit = MINUTES)
    void shouldBeAbleToProgressUnderStressfulProcessorChangesWhenUnordered() throws Exception {
        shouldBeAbleToProgressUnderStressfulProcessorChanges(0);
    }

    private static void shouldBeAbleToProgressUnderStressfulProcessorChanges(int orderingGuarantees) throws Exception {
        // given
        int batches = 100;
        int processors = Runtime.getRuntime().availableProcessors() * 10;
        Configuration config = new Configuration.Overridden(Configuration.DEFAULT) {
            @Override
            public int maxNumberOfWorkerThreads() {
                return processors;
            }
        };
        Stage stage = new StressStage(config, orderingGuarantees, batches);
        StageExecution execution = stage.execute();
        List<Step<?>> steps = asList(execution.steps());
        steps.get(1).processors(processors / 3);

        // when
        ThreadLocalRandom random = ThreadLocalRandom.current();
        while (execution.stillExecuting()) {
            steps.get(2).processors(random.nextInt(-2, 5));
            Thread.sleep(1);
        }
        execution.assertHealthy();

        // then
        assertEquals(
                batches,
                steps.get(steps.size() - 1).stats().stat(Keys.done_batches).asLong());

        closeAllSilently(steps);
    }

    private static class StressStage extends Stage {
        StressStage(Configuration config, int orderingGuarantees, int batches) {
            super("Stress", null, config, orderingGuarantees);

            add(new PullingProducerStep<>(control(), config) {
                @Override
                protected long position() {
                    return 0;
                }

                @Override
                protected Object nextBatchOrNull(long ticket, int batchSize, ProcessContext processContext) {
                    return ticket < batches ? ticket : null;
                }
            });
            add(new ProcessorStep<Long>(control(), "Yeah", config, 3, CONTEXT_FACTORY) {
                @Override
                protected void process(Long batch, BatchSender sender, CursorContext cursorContext) throws Throwable {
                    Thread.sleep(1);
                    sender.send(batch);
                }
            });
            add(new ForkedProcessorStep<Long>(control(), "Subject", config) {
                @Override
                protected void forkedProcess(int id, int processors, Long batch) throws Throwable {
                    Thread.sleep(1);
                }
            });
            add(new DeadEndStep(control(), CONTEXT_FACTORY));
        }
    }

    private static class BatchProcessor extends ForkedProcessorStep<Batch> {
        BatchProcessor(StageControl control, int processors) {
            super(control, "PROCESSOR", config(processors));
        }

        @Override
        protected void forkedProcess(int id, int processors, Batch batch) {
            batch.processedBy(id);
        }
    }

    private static Configuration config(int processors) {
        return new Configuration() {
            @Override
            public int maxNumberOfWorkerThreads() {
                return processors;
            }
        };
    }

    private static class Batch {
        private final boolean[] processed;

        Batch(int processors) {
            this.processed = new boolean[processors];
        }

        void processedBy(int id) {
            assertFalse(processed[id]);
            processed[id] = true;
        }
    }

    private static class TrackingStep implements Step<Batch> {
        private final AtomicLong received = new AtomicLong();

        @Override
        public void receivePanic(Throwable cause) {}

        @Override
        public void start(int orderingGuarantees) {}

        @Override
        public String name() {
            return "END";
        }

        @Override
        public long receive(long ticket, Batch batch) {
            int count = 0;
            for (int i = 0; i < batch.processed.length; i++) {
                if (batch.processed[i]) {
                    count++;
                }
            }
            assertEquals(batch.processed.length, count);
            if (!received.compareAndSet(ticket - 1, ticket)) {
                fail("Hmm " + ticket + " " + received.get());
            }
            return 0;
        }

        @Override
        public StepStats stats() {
            return null;
        }

        @Override
        public void endOfUpstream() {}

        @Override
        public boolean isCompleted() {
            return false;
        }

        @Override
        public boolean awaitCompleted(long time, TimeUnit unit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setDownstream(Step<?> downstreamStep) {}

        @Override
        public void close() {}
    }
}
