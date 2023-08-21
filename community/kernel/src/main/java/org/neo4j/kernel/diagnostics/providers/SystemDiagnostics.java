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
package org.neo4j.kernel.diagnostics.providers;

import static java.lang.String.format;
import static java.net.NetworkInterface.getNetworkInterfaces;
import static org.neo4j.io.ByteUnit.bytesToString;

import java.io.File;
import java.io.UncheckedIOException;
import java.lang.management.CompilationMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.zone.ZoneRulesProvider;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.neo4j.internal.diagnostics.DiagnosticsLogger;
import org.neo4j.internal.diagnostics.DiagnosticsProvider;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.util.VisibleForTesting;

public enum SystemDiagnostics implements DiagnosticsProvider {
    SYSTEM_MEMORY("System memory information") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            logBytes(logger, "Total Physical memory: ", OsBeanUtil.getTotalPhysicalMemory());
            logBytes(logger, "Free Physical memory: ", OsBeanUtil.getFreePhysicalMemory());
            logBytes(logger, "Committed virtual memory: ", OsBeanUtil.getCommittedVirtualMemory());
            logBytes(logger, "Total swap space: ", OsBeanUtil.getTotalSwapSpace());
            logBytes(logger, "Free swap space: ", OsBeanUtil.getFreeSwapSpace());
        }
    },
    JAVA_MEMORY("JVM memory information") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            logger.log("Free  memory: " + bytesToString(Runtime.getRuntime().freeMemory()));
            logger.log("Total memory: " + bytesToString(Runtime.getRuntime().totalMemory()));
            logger.log("Max   memory: " + bytesToString(Runtime.getRuntime().maxMemory()));
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                logger.log("Garbage Collector: " + gc.getName() + ": " + Arrays.toString(gc.getMemoryPoolNames()));
            }
            for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
                MemoryUsage usage = pool.getUsage();
                logger.log(format(
                        "Memory Pool: %s (%s): committed=%s, used=%s, max=%s, threshold=%s",
                        pool.getName(),
                        pool.getType(),
                        usage == null ? "?" : bytesToString(usage.getCommitted()),
                        usage == null ? "?" : bytesToString(usage.getUsed()),
                        usage == null ? "?" : bytesToString(usage.getMax()),
                        pool.isUsageThresholdSupported() ? bytesToString(pool.getUsageThreshold()) : "?"));
            }
        }
    },
    OPERATING_SYSTEM("Operating system information") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
            logger.log(format(
                    "Operating System: %s; version: %s; arch: %s; cpus: %s",
                    os.getName(), os.getVersion(), os.getArch(), os.getAvailableProcessors()));
            logLong(logger, "Max number of file descriptors: ", OsBeanUtil.getMaxFileDescriptors());
            logLong(logger, "Number of open file descriptors: ", OsBeanUtil.getOpenFileDescriptors());
            logger.log("Process id: " + ProcessHandle.current().pid());
            logger.log("Byte order: " + ByteOrder.nativeOrder());
            logger.log("Local timezone: " + getLocalTimeZone());
            logger.log("Memory page size: " + UnsafeUtil.pageSize());
            logger.log("Unaligned memory access allowed: " + UnsafeUtil.allowUnalignedMemoryAccess);
        }

        private String getLocalTimeZone() {
            return ZoneId.systemDefault().getId();
        }
    },
    JAVA_VIRTUAL_MACHINE("JVM information") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            logger.log("VM Name: " + runtime.getVmName());
            logger.log("VM Vendor: " + runtime.getVmVendor());
            logger.log("VM Version: " + runtime.getVmVersion());
            CompilationMXBean compiler = ManagementFactory.getCompilationMXBean();
            logger.log("JIT compiler: " + ((compiler == null) ? "unknown" : compiler.getName()));
            logger.log("VM Arguments: " + runtime.getInputArguments());
        }
    },
    CLASSPATH("Java classpath") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            Collection<String> classpath;
            if (runtime.isBootClassPathSupported()) {
                classpath = buildClassPath(
                        getClass().getClassLoader(),
                        Map.of("bootstrap", runtime.getBootClassPath(), "classpath", runtime.getClassPath()));
            } else {
                classpath = buildClassPath(getClass().getClassLoader(), Map.of("classpath", runtime.getClassPath()));
            }
            for (String path : classpath) {
                logger.log(path);
            }
        }

        private Collection<String> buildClassPath(ClassLoader loader, Map<String, String> classPaths) {
            Map<String, String> paths = new HashMap<>();

            Set<Entry<String, String>> entries = classPaths.entrySet();
            for (Entry<String, String> classPathEntry : entries) {
                String classPathType = classPathEntry.getKey();
                String[] splittedClassPath = classPathEntry.getValue().split(File.pathSeparator);
                for (String entry : splittedClassPath) {
                    String canonicalEntry = canonicalize(entry);
                    paths.merge(canonicalEntry, classPathType, (k, v) -> v + " + " + classPathType);
                }
            }

            for (int level = 0; loader != null; level++) {
                if (loader instanceof URLClassLoader urls) {
                    URL[] classLoaderUrls = urls.getURLs();
                    if (classLoaderUrls != null) {
                        for (URL url : classLoaderUrls) {
                            if ("file".equalsIgnoreCase(url.getProtocol())) {
                                String type = "loader." + level;
                                paths.merge(url.toString(), type, (k, v) -> k + " + " + type);
                            }
                        }
                    } else {
                        paths.put(loader.toString(), "<ClassLoader unexpectedly has null URL array>");
                    }
                }
                loader = loader.getParent();
            }
            List<String> result = new ArrayList<>(paths.size());
            for (Entry<String, String> path : paths.entrySet()) {
                result.add(" [" + path.getValue() + "] " + path.getKey());
            }
            return result;
        }
    },
    LIBRARY_PATH("Library path") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
            for (String path : runtime.getLibraryPath().split(File.pathSeparator)) {
                logger.log(canonicalize(path));
            }
        }
    },
    SYSTEM_PROPERTIES("System properties") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            for (Object property : System.getProperties().keySet()) {
                if (property instanceof String key) {
                    if (key.startsWith("java.")
                            || key.startsWith("os.")
                            || key.endsWith(".boot.class.path")
                            || key.equals("line.separator")) {
                        continue;
                    }
                    logger.log(key + " = " + System.getProperty(key));
                }
            }
        }
    },
    TIMEZONE_DATABASE("(IANA) TimeZone database version") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            Map<String, Integer> versions = new HashMap<>();
            for (String tz : ZoneRulesProvider.getAvailableZoneIds()) {
                for (String version : ZoneRulesProvider.getVersions(tz).keySet()) {
                    versions.compute(version, (key, value) -> value == null ? 1 : (value + 1));
                }
            }
            String[] sorted = versions.keySet().toArray(new String[0]);
            Arrays.sort(sorted);
            for (String tz : sorted) {
                logger.log(format("  TimeZone version: %s (available for %d zone identifiers)", tz, versions.get(tz)));
            }
        }
    },
    NETWORK("Network information") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            try {
                Enumeration<NetworkInterface> networkInterfaces = getNetworkInterfaces();

                while (networkInterfaces.hasMoreElements()) {
                    NetworkInterface iface = networkInterfaces.nextElement();
                    logger.log(format("Interface %s:", iface.getDisplayName()));

                    Enumeration<InetAddress> addresses = iface.getInetAddresses();
                    while (addresses.hasMoreElements()) {
                        InetAddress address = addresses.nextElement();
                        String hostAddress = address.getHostAddress();
                        logger.log(format("    address: %s", hostAddress));
                    }
                }
            } catch (SocketException e) {
                logger.log("ERROR: failed to inspect network interfaces and addresses: " + e.getMessage());
            }
        }
    },
    NATIVE_ACCESSOR("Native access information") {
        @Override
        public void dump(DiagnosticsLogger logger) {
            NativeAccess nativeAccess = NativeAccessProvider.getNativeAccess();
            logger.log("Native access details: " + nativeAccess.describe());
        }
    };

    private final String name;

    SystemDiagnostics(String name) {
        this.name = name;
    }

    @Override
    public String getDiagnosticsName() {
        return name;
    }

    @VisibleForTesting
    static String canonicalize(String path) {
        try {
            boolean hasWildcard = path.endsWith("*");
            if (hasWildcard) {
                path = path.substring(0, path.length() - 1);
            }
            String result =
                    FileUtils.getCanonicalFile(Path.of(path)).toAbsolutePath().toString();
            if (hasWildcard) {
                result += File.separator + "*";
            }
            return result;
        } catch (UncheckedIOException e) {
            return Path.of(path).toAbsolutePath().toString();
        }
    }

    private static void logBytes(DiagnosticsLogger logger, String message, long value) {
        if (value != OsBeanUtil.VALUE_UNAVAILABLE) {
            logger.log(message + bytesToString(value));
        }
    }

    private static void logLong(DiagnosticsLogger logger, String message, long value) {
        if (value != OsBeanUtil.VALUE_UNAVAILABLE) {
            logger.log(message + value);
        }
    }
}
