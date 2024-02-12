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
package org.neo4j.dbms.diagnostics.jmx;

import static java.nio.file.Files.createTempFile;
import static java.nio.file.Files.deleteIfExists;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import jdk.jfr.Configuration;
import jdk.management.jfr.FlightRecorderMXBean;
import org.neo4j.internal.utils.DumpUtils;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSource;
import org.neo4j.kernel.diagnostics.DiagnosticsReportSources;

/**
 * Encapsulates remoting functionality for collecting diagnostics information on running instances.
 */
public class JmxDump implements AutoCloseable {
    private final JMXConnector connector;
    private final MBeanServerConnection mBeanServer;
    private final long pid;
    private Properties systemProperties;
    public static final String THREAD_DUMP_FAILURE = "ERROR: Unable to produce any thread dump";
    public static final String VM_ARGS_FAILURE = "ERROR: Unable to produce any thread dump";

    private JmxDump(JMXConnector connector, MBeanServerConnection mBeanServer, long pid) {
        this.connector = connector;
        this.mBeanServer = mBeanServer;
        this.pid = pid;
    }

    public static JmxDump connectTo(String jmxAddress, long pid) throws IOException {
        JMXServiceURL url = new JMXServiceURL(jmxAddress);
        JMXConnector connect = JMXConnectorFactory.connect(url);

        return new JmxDump(connect, connect.getMBeanServerConnection(), pid);
    }

    public void attachSystemProperties(Properties systemProperties) {
        this.systemProperties = systemProperties;
    }

    /**
     * Captures a thread dump of the running instance.
     *
     * @return a diagnostics source the will emit a thread dump.
     */
    public DiagnosticsReportSource threadDumpSource() {
        return DiagnosticsReportSources.newDiagnosticsString("threaddump.txt", this::threadDump);
    }

    public String threadDump() {
        String result;
        try {
            // Try to invoke real thread dump
            result = (String) mBeanServer.invoke(
                    new ObjectName("com.sun.management:type=DiagnosticCommand"),
                    "threadPrint",
                    new Object[] {null},
                    new String[] {String[].class.getName()});
        } catch (InstanceNotFoundException
                | ReflectionException
                | MBeanException
                | MalformedObjectNameException
                | IOException exception) {
            // Failed, do a poor mans attempt
            result = getMXThreadDump();
        }

        return result;
    }

    /**
     * If "DiagnosticCommand" bean isn't available, for reasons unknown, try our best to reproduce the output. For obvious
     * reasons we can't get everything, since it's not exposed in java.
     *
     * @return a thread dump.
     */
    private String getMXThreadDump() {
        ThreadMXBean threadMxBean;
        try {
            threadMxBean = ManagementFactory.getPlatformMXBean(mBeanServer, ThreadMXBean.class);
        } catch (IOException e) {
            return THREAD_DUMP_FAILURE;
        }

        return DumpUtils.threadDump(threadMxBean, systemProperties);
    }

    public String vmArguments() {
        RuntimeMXBean runtimeMxBean;
        try {
            runtimeMxBean = ManagementFactory.getPlatformMXBean(mBeanServer, RuntimeMXBean.class);
        } catch (IOException e) {
            return VM_ARGS_FAILURE;
        }
        return String.join(" ", runtimeMxBean.getInputArguments());
    }

    public DiagnosticsReportSource heapDump() {
        return new DiagnosticsReportSource() {
            @Override
            public String destinationPath() {
                return "heapdump.hprof";
            }

            @Override
            public InputStream newInputStream() throws IOException {
                final Path heapdumpFile =
                        createTempFile("neo4j-heapdump", ".hprof").toAbsolutePath();
                deleteIfExists(heapdumpFile);
                heapDump(heapdumpFile.toString());
                return new FileInputStream(heapdumpFile.toFile()) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        deleteIfExists(heapdumpFile);
                    }
                };
            }

            @Override
            public long estimatedSize() {
                try {
                    final MemoryMXBean bean = ManagementFactory.getPlatformMXBean(mBeanServer, MemoryMXBean.class);
                    final long totalMemory = bean.getHeapMemoryUsage().getCommitted()
                            + bean.getNonHeapMemoryUsage().getCommitted();

                    // We first write raw to disk then write to archive, 5x compression is a reasonable worst case
                    // estimation
                    return (long) (totalMemory * 1.2);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    /**
     * @param destination file path to send heap dump to, has to end with ".hprof"
     */
    private void heapDump(String destination) throws IOException {
        HotSpotDiagnosticMXBean hotSpotDiagnosticMXBean =
                ManagementFactory.getPlatformMXBean(mBeanServer, HotSpotDiagnosticMXBean.class);
        hotSpotDiagnosticMXBean.dumpHeap(destination, false);
    }

    public DiagnosticsReportSource systemProperties() {
        return new DiagnosticsReportSource() {
            @Override
            public String destinationPath() {
                return "vm.prop";
            }

            @Override
            public InputStream newInputStream() {
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                systemProperties.list(new PrintStream(out, true));
                return new ByteArrayInputStream(out.toByteArray());
            }

            @Override
            public long estimatedSize() {
                return 0;
            }
        };
    }

    public JfrProfileConnection jfrConnection() throws IOException {
        FlightRecorderMXBean bean = ManagementFactory.getPlatformMXBean(mBeanServer, FlightRecorderMXBean.class);
        return new JfrProfileConnection(bean);
    }

    @Override
    public void close() {
        try {
            connector.close();
        } catch (IOException ignored) {
        }
    }

    public long getPid() {
        return pid;
    }

    public static class JfrProfileConnection {
        private final FlightRecorderMXBean bean;
        private final Map<String, String> settings;
        private boolean hasRecording;
        private long recordingId;
        private static final Set<String> RUNNING_STATES = Set.of("STARTING", "RUNNING", "STOPPING");

        private JfrProfileConnection(FlightRecorderMXBean bean) {
            this.bean = bean;
            try {
                // Typically two configurations exists. "default" with 1% overhead and "profile" with 2% overhead
                settings = Configuration.getConfiguration("profile").getSettings();
            } catch (Exception e) {
                // Should we have a static config map here as a fallback instead?
                throw new RuntimeException("No configuration found for JFR");
            }
        }

        public void start(String name, Duration maxDuration, Path path) {
            if (hasRecording) {
                throw new IllegalStateException("Already has a running recording, needs to be stopped first");
            }
            recordingId = bean.newRecording();
            Map<String, String> config = Map.of(
                    "name", name,
                    "duration", maxDuration.getSeconds() + "s",
                    "destination", path.toAbsolutePath().toString(),
                    "disk", "true",
                    "maxAge", "0s", // Unlimited
                    "maxSize", "0", // Unlimited
                    "dumpOnExit", "true");
            bean.setRecordingOptions(recordingId, config);
            bean.setRecordingSettings(recordingId, settings);
            bean.startRecording(recordingId);
            hasRecording = true;
        }

        public void stop() {
            if (hasRecording) {
                try {
                    bean.closeRecording(recordingId);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                } finally {
                    hasRecording = false;
                }
            }
        }

        public boolean isRunning() {
            if (hasRecording) {
                try {
                    // Valid return values are "NEW", "DELAYED", "STARTING", "RUNNING", "STOPPING", "STOPPED" and
                    // "CLOSED".
                    return bean.getRecordings().stream()
                            .anyMatch(info -> info.getId() == recordingId && RUNNING_STATES.contains(info.getState()));
                } catch (RuntimeException ignored) {
                    // Exception here likely means there is a connection issue with the server
                }
            }
            return false;
        }
    }
}
