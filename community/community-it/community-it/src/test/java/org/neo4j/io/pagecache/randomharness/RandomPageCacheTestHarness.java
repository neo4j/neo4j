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
package org.neo4j.io.pagecache.randomharness;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.adversaries.RandomAdversary;
import org.neo4j.adversaries.fs.AdversarialFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.PageSwapperFactory;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.linear.LinearHistoryPageCacheTracerTest;
import org.neo4j.io.pagecache.tracing.linear.LinearTracers;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.DaemonThreadFactory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

/**
 * The RandomPageCacheTestHarness can plan and run random page cache tests, repeatably if necessary, and verify that
 * the behaviour of the page cache is correct to some degree. For instance, it can verify that records don't end up
 * overlapping each other in the mapped files, that records end up at the correct locations in the files, and that
 * records don't end up in the wrong files. The harness can also execute separate preparation and verification steps,
 * before and after executing the planned test respectively, and it can integrate with the adversarial file system
 * for fault injection, and arbitrary PageCacheTracers.
 * <p>
 * See {@link LinearHistoryPageCacheTracerTest} for an example of how to configure and use the harness.
 */
public class RandomPageCacheTestHarness implements Closeable {
    private final ExecutorService executorService = new ThreadPoolExecutor(
            0, Integer.MAX_VALUE, 1, TimeUnit.SECONDS, new SynchronousQueue<>(), new DaemonThreadFactory());

    private double mischiefRate;
    private double failureRate;
    private double errorRate;
    private int concurrencyLevel;
    private int initialMappedFiles;
    private int cachePageCount;
    private int filePageCount;
    private int filePageSize;
    private PageCacheTracer tracer;
    private int commandCount;
    private final double[] commandProbabilityFactors;
    private long randomSeed;
    private boolean fixedRandomSeed;
    private FileSystemAbstraction fs;
    private boolean useAdversarialIO;
    private Plan plan;
    private Phase preparation;
    private Phase verification;
    private RecordFormat recordFormat;
    private Path basePath;
    private ImmutableSet<OpenOption> openOptions;

    public RandomPageCacheTestHarness() {
        mischiefRate = 0.1;
        failureRate = 0.1;
        errorRate = 0.0;
        concurrencyLevel = 1;
        initialMappedFiles = 2;
        cachePageCount = 20;
        filePageCount = cachePageCount * 10;
        tracer = PageCacheTracer.NULL;
        commandCount = 1000;

        Command[] commands = Command.values();
        commandProbabilityFactors = new double[commands.length];
        for (Command command : commands) {
            commandProbabilityFactors[command.ordinal()] = command.getDefaultProbabilityFactor();
        }

        fs = new EphemeralFileSystemAbstraction();
        useAdversarialIO = true;
        recordFormat = new StandardRecordFormat();
        basePath = Path.of("random-harness-default-path");
        openOptions = Sets.immutable.empty();
    }

    /**
     * Disable all of the given commands, by setting their probability factors to zero.
     */
    public void disableCommands(Command... commands) {
        for (Command command : commands) {
            setCommandProbabilityFactor(command, 0);
        }
    }

    /**
     * Set the probability factor of the given command. The default value is given by
     * {@link Command#getDefaultProbabilityFactor()}. The effective probability is computed from the relative
     * difference in probability factors between all the commands.
     * <p>
     * Setting the probability factor to zero will disable that command.
     */
    public void setCommandProbabilityFactor(Command command, double probabilityFactor) {
        assert 0.0 <= probabilityFactor : "Probability factor cannot be negative";
        commandProbabilityFactors[command.ordinal()] = probabilityFactor;
    }

    /**
     * Set to "true" to execute the plans with fault injection from the {@link AdversarialFileSystemAbstraction}, or
     * set to "false" to disable this feature.
     * <p>
     * The default is "true".
     */
    public void setUseAdversarialIO(boolean useAdversarialIO) {
        this.useAdversarialIO = useAdversarialIO;
    }

    /**
     * Set the PageCacheTracer that the page cache under test should be configured with.
     */
    public void setTracer(PageCacheTracer tracer) {
        this.tracer = tracer;
    }

    /**
     * Set the mischief rate for the adversarial file system.
     */
    public void setMischiefRate(double rate) {
        mischiefRate = rate;
    }

    /**
     * Set the failure rate for the adversarial file system.
     */
    public void setFailureRate(double rate) {
        failureRate = rate;
    }

    /**
     * Set the error rate for the adversarial file system.
     */
    public void setErrorRate(double rate) {
        errorRate = rate;
    }

    /**
     * Set the number of threads that will execute commands from the plan. If this number is greater than 1, then the
     * plan will execute non-deterministically. The description of the iteration that
     * {@link #describePreviousRun(PrintStream)} prints will include which thread performed which command.
     */
    public void setConcurrencyLevel(int concurrencyLevel) {
        this.concurrencyLevel = concurrencyLevel;
    }

    /**
     * Set the number of files that should be mapped from the start of the plan. If you have set the probability of
     * the {@link Command#MapFile} command to zero, then you must have a positive number of initial mapped files.
     * Otherwise there will be no files to plan any work for.
     * <p>
     * The default value is 2.
     */
    public void setInitialMappedFiles(int initialMappedFiles) {
        this.initialMappedFiles = initialMappedFiles;
    }

    public void setCachePageCount(int count) {
        this.cachePageCount = count;
    }

    public void setFilePageCount(int count) {
        this.filePageCount = count;
    }

    public void setFilePageSize(int size) {
        this.filePageSize = size;
    }

    /**
     * Set the number of commands to plan in each iteration.
     */
    public void setCommandCount(int commandCount) {
        this.commandCount = commandCount;
    }

    /**
     * Set the preparation phase to use. This phase is executed before all the planned commands. It can be used to
     * prepare some file contents, or reset some external state, such as the
     * {@link LinearTracers}.
     * <p>
     * The preparation phase is executed before each iteration.
     */
    public void setPreparation(Phase preparation) {
        this.preparation = preparation;
    }

    /**
     * Set the verification phase to use. This phase is executed after all the planned commands have executed
     * completely, and can be used to verify the consistency of the data, or some other invariant.
     * <p>
     * The verification phase is executed after each iteration.
     */
    public void setVerification(Phase verification) {
        this.verification = verification;
    }

    /**
     * Set the record format to use. The record format is used to read, write and verify file contents.
     */
    public void setRecordFormat(RecordFormat recordFormat) {
        this.recordFormat = recordFormat;
    }

    /**
     * Set and fix the random seed to the given value. All iterations run through this harness will then use that seed.
     * <p>
     * If the random seed has not been configured, then each iteration will use a new seed.
     */
    public void setRandomSeed(long randomSeed) {
        this.randomSeed = randomSeed;
        this.fixedRandomSeed = true;
    }

    public void setFileSystem(FileSystemAbstraction fileSystem) {
        this.fs = fileSystem;
    }

    public void setBasePath(Path basePath) {
        this.basePath = basePath;
    }

    public void setOpenOptions(ImmutableSet<OpenOption> openOptions) {
        this.openOptions = openOptions;
    }

    /**
     * Write out a textual description of the last run iteration, including the exact plan and what thread
     * executed which command, and the random seed that can be used to recreate that plan for improved repeatability.
     */
    public void describePreviousRun(PrintStream out) {
        out.println("randomSeed = " + randomSeed);
        out.println("commandCount = " + commandCount);
        out.println("concurrencyLevel (number of worker threads) = " + concurrencyLevel);
        out.println("initialMappedFiles = " + initialMappedFiles);
        out.println("cachePageCount = " + cachePageCount);
        out.println("tracer = " + tracer);
        out.println("useAdversarialIO = " + useAdversarialIO);
        out.println("mischeifRate = " + mischiefRate);
        out.println("failureRate = " + failureRate);
        out.println("errorRate = " + errorRate);
        out.println("Command probability factors:");
        Command[] commands = Command.values();
        for (int i = 0; i < commands.length; i++) {
            out.print("  ");
            out.print(commands[i]);
            out.print(" = ");
            out.println(commandProbabilityFactors[i]);
        }
        if (plan != null) {
            plan.print(out);
        }
    }

    /**
     * Run a single iteration with the current harness configuration.
     * <p>
     * This will either complete within the given timeout, or throw an exception.
     * <p>
     * If the run fails, then a description will be printed to System.err.
     */
    public void run(long iterationTimeout, TimeUnit unit) throws Exception {
        run(1, iterationTimeout, unit);
    }

    /**
     * Run the given number of iterations with the given harness configuration.
     * <p>
     * If the random seed has been set to a specific value, then all iterations will use that seed. Otherwise each
     * iteration will use a new seed.
     * <p>
     * The given timeout applies to the individual iteration, not to their combined run. This is effectively similar
     * to calling {@link #run(long, TimeUnit)} the given number of times.
     * <p>
     * The run will stop at the first failure, if any, and print a description of it to System.err.
     */
    public void run(int iterations, long iterationTimeout, TimeUnit unit) throws Exception {
        try {
            for (int i = 0; i < iterations; i++) {
                runIteration(iterationTimeout, unit);
            }
        } catch (Exception e) {
            describePreviousRun(System.err);
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        fs.close();
        executorService.shutdown();
    }

    @SuppressWarnings("unchecked")
    private void runIteration(long timeout, TimeUnit unit) throws Exception {
        assert filePageSize % recordFormat.getRecordSize() == 0
                : "File page size must be a multiple of the record size";

        if (!fixedRandomSeed) {
            randomSeed = ThreadLocalRandom.current().nextLong();
        }

        FileSystemAbstraction fs = this.fs;
        Path[] files = buildFileNames();

        RandomAdversary adversary = new RandomAdversary(mischiefRate, failureRate, errorRate);
        adversary.enableAdversary(false);
        if (useAdversarialIO) {
            adversary.setSeed(randomSeed);
            fs = new AdversarialFileSystemAbstraction(adversary, fs);
        }

        PageSwapperFactory swapperFactory = new SingleFilePageSwapperFactory(fs, tracer, EmptyMemoryTracker.INSTANCE);
        JobScheduler jobScheduler = new ThreadPoolJobScheduler();
        MuninnPageCache cache = new MuninnPageCache(
                swapperFactory,
                jobScheduler,
                MuninnPageCache.config(cachePageCount).pageCacheTracer(tracer));
        if (filePageSize == 0) {
            filePageSize = cache.pageSize();
        }
        cache.setPrintExceptionsOnClose(false);
        Map<Path, PagedFile> fileMap = new HashMap<>(files.length);
        for (int i = 0; i < Math.min(files.length, initialMappedFiles); i++) {
            Path file = files[i];
            fileMap.put(file, cache.map(file, filePageSize, DEFAULT_DATABASE_NAME, openOptions));
        }

        plan = plan(cache, files, fileMap);

        AtomicBoolean stopSignal = new AtomicBoolean();
        Callable<Void> planRunner = new PlanRunner(plan, stopSignal);
        Future<Void>[] futures = new Future[concurrencyLevel];
        for (int i = 0; i < concurrencyLevel; i++) {
            futures[i] = executorService.submit(planRunner);
        }

        if (preparation != null) {
            preparation.run(cache, this.fs, plan.getFilesTouched());
        }

        adversary.enableAdversary(true);

        plan.start();

        long deadlineMillis = System.currentTimeMillis() + unit.toMillis(timeout);
        long now;
        try {
            for (Future<Void> future : futures) {
                now = System.currentTimeMillis();
                if (deadlineMillis < now) {
                    throw new TimeoutException();
                }
                future.get(deadlineMillis - now, TimeUnit.MILLISECONDS);
            }
            adversary.enableAdversary(false);
            runVerificationPhase(cache);
        } finally {
            stopSignal.set(true);
            adversary.enableAdversary(false);
            try {
                for (Future<Void> future : futures) {
                    future.get(10, TimeUnit.SECONDS);
                }
            } catch (InterruptedException | TimeoutException e) {
                for (Future<Void> future : futures) {
                    future.cancel(true);
                }
                throw new RuntimeException(e);
            }

            try {
                plan.close();
                cache.close();
                jobScheduler.close();

                if (this.fs instanceof EphemeralFileSystemAbstraction) {
                    this.fs.close();
                    this.fs = new EphemeralFileSystemAbstraction();
                } else {
                    for (Path file : files) {
                        Files.delete(file);
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void runVerificationPhase(MuninnPageCache cache) throws Exception {
        if (verification != null) {
            cache.flushAndForce(DatabaseFlushEvent.NULL); // Clears any stray evictor exceptions
            verification.run(cache, this.fs, plan.getFilesTouched());
        }
    }

    private Path[] buildFileNames() throws IOException {
        String s = "abcdefghijklmnopqrstuvwxyz";
        Path[] files = new Path[s.length()];
        for (int i = 0; i < s.length(); i++) {
            files[i] = basePath.resolve(s.substring(i, i + 1)).normalize();
            fs.mkdirs(files[i].getParent());
            StoreChannel channel = fs.write(files[i]);
            channel.truncate(0);
            channel.close();
        }
        return files;
    }

    private Plan plan(MuninnPageCache cache, Path[] files, Map<Path, PagedFile> fileMap) {
        Action[] plan = new Action[commandCount];

        int[] commandWeights = computeCommandWeights();
        int commandWeightSum = sum(commandWeights);
        Random rng = new Random(randomSeed);
        var primer =
                new CommandPrimer(rng, cache, files, fileMap, filePageCount, filePageSize, recordFormat, openOptions);

        for (int i = 0; i < plan.length; i++) {
            Command command = pickCommand(rng.nextInt(commandWeightSum), commandWeights);
            Action action = primer.prime(command);
            plan[i] = action;
            if (action == null) {
                i--;
            }
        }

        return new Plan(plan, fileMap, primer.getMappedFiles(), primer.getFilesTouched());
    }

    private int[] computeCommandWeights() {
        Command[] commands = Command.values();
        int[] weights = new int[commands.length];

        int base = 100_000_000;
        for (int i = 0; i < commands.length; i++) {
            weights[i] = (int) (base * commandProbabilityFactors[i]);
        }

        return weights;
    }

    private static int sum(int[] xs) {
        int sum = 0;
        for (int x : xs) {
            sum += x;
        }
        return sum;
    }

    private static Command pickCommand(int randomPick, int[] commandWeights) {
        for (int i = 0; i < commandWeights.length; i++) {
            randomPick -= commandWeights[i];
            if (randomPick < 0) {
                return Command.values()[i];
            }
        }
        throw new AssertionError(
                "Tried to pick randomPick = " + randomPick + " from weights = " + Arrays.toString(commandWeights));
    }
}
