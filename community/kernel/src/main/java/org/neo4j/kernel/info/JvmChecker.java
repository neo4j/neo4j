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
package org.neo4j.kernel.info;

import static java.util.regex.Pattern.compile;
import static org.neo4j.configuration.BootloaderSettings.initial_heap_size;
import static org.neo4j.configuration.BootloaderSettings.max_heap_size;

import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import org.eclipse.collections.api.factory.primitive.IntSets;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.neo4j.logging.InternalLog;

public class JvmChecker {
    public static final Pattern SUPPORTED_JAVA_NAME_PATTERN =
            compile("(Java HotSpot\\(TM\\)|OpenJDK) (64-Bit Server|Server) VM");
    public static final String NEO4J_JAVA_WARNING_MESSAGE = "Please use Java(TM) 17 or Java(TM) 21 to run Neo4j.";
    private static final IntSet SUPPORTED_JVM_VERSIONS = IntSets.immutable.of(17, 21);
    static final String INCOMPATIBLE_JVM_WARNING =
            "You are using an unsupported Java runtime. " + NEO4J_JAVA_WARNING_MESSAGE;
    static final String INCOMPATIBLE_JVM_VERSION_WARNING =
            "You are using an unsupported version of the Java runtime. " + NEO4J_JAVA_WARNING_MESSAGE;

    private final InternalLog log;
    private final JvmMetadataRepository jvmMetadataRepository;

    public JvmChecker(InternalLog log, JvmMetadataRepository jvmMetadataRepository) {
        this.log = log;
        this.jvmMetadataRepository = jvmMetadataRepository;
    }

    public void checkJvmCompatibilityAndIssueWarning() {
        String javaVmName = jvmMetadataRepository.getJavaVmName();
        Runtime.Version javaVersion = jvmMetadataRepository.getJavaVersion();

        if (!SUPPORTED_JAVA_NAME_PATTERN.matcher(javaVmName).matches()) {
            log.warn(INCOMPATIBLE_JVM_WARNING);
        } else if (!SUPPORTED_JVM_VERSIONS.contains(javaVersion.feature())) {
            log.warn(INCOMPATIBLE_JVM_VERSION_WARNING);
        }
        List<String> jvmArguments = jvmMetadataRepository.getJvmInputArguments();
        MemoryUsage heapMemoryUsage = jvmMetadataRepository.getHeapMemoryUsage();
        if (missingOption(jvmArguments, "-Xmx")) {
            log.warn(maxMemorySettingWarning(heapMemoryUsage.getMax()));
        }
        if (missingOption(jvmArguments, "-Xms")) {
            log.warn(initialMemorySettingWarning(heapMemoryUsage.getInit()));
        }
    }

    static String initialMemorySettingWarning(long currentUsage) {
        return String.format(
                "The initial heap memory has not been configured. It is recommended that it is always explicitly configured, to "
                        + "ensure the system has a balanced configuration. Until then, a JVM computed heuristic of %d bytes is used instead. If you are running "
                        + "neo4j server, you need to configure %s in neo4j.conf. If you are running neo4j embedded, you have to launch the JVM with -Xms set to a "
                        + "value. You can run neo4j-admin server memory-recommendation for memory configuration suggestions.",
                currentUsage, initial_heap_size.name());
    }

    static String maxMemorySettingWarning(long currentUsage) {
        return String.format(
                "The max heap memory has not been configured. It is recommended that it is always explicitly configured, to "
                        + "ensure the system has a balanced configuration. Until then, a JVM computed heuristic of %d bytes is used instead. If you are running "
                        + "neo4j server, you need to configure %s in neo4j.conf. If you are running neo4j embedded, you have to launch the JVM with -Xmx set to a "
                        + "value. You can run neo4j-admin server memory-recommendation for memory configuration suggestions.",
                currentUsage, max_heap_size.name());
    }

    private static boolean missingOption(List<String> jvmArguments, String option) {
        String normalizedOption = option.toUpperCase(Locale.ROOT);
        return jvmArguments.stream().noneMatch(o -> o.toUpperCase(Locale.ROOT).startsWith(normalizedOption));
    }
}
