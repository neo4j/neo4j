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
package org.neo4j.test.arguments;

import static org.neo4j.util.Preconditions.checkArgument;

import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.support.AnnotationConsumer;
import org.neo4j.kernel.KernelVersion;

public class KernelVersionArgumentsProvider implements ArgumentsProvider, AnnotationConsumer<KernelVersionSource> {
    private KernelVersionSource kernelVersionSource;

    @Override
    public void accept(KernelVersionSource kernelVersionSource) {
        this.kernelVersionSource = kernelVersionSource;
    }

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext extensionContext) {
        Set<KernelVersion> versions = new TreeSet<>(KernelVersion.VERSIONS);
        Version lessThan = getLessThan();
        Version greaterThan = getGreaterThan();

        if (lessThan.compareTo(greaterThan) > 0) {
            versions.removeIf(k -> fromKernelVersion(k).compareTo(lessThan) >= 0
                    || fromKernelVersion(k).compareTo(greaterThan) <= 0);
        } else {
            versions.removeIf(k -> fromKernelVersion(k).compareTo(lessThan) >= 0
                    && fromKernelVersion(k).compareTo(greaterThan) <= 0);
        }

        return versions.stream().map(Arguments::of);
    }

    private Version getLessThan() {
        String lessThan = kernelVersionSource.lessThan();
        if (lessThan.isEmpty()) {
            return new Version(Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
        return fromString(lessThan);
    }

    private Version getGreaterThan() {
        String greaterThan = kernelVersionSource.greaterThan();
        String atLeast = kernelVersionSource.atLeast();

        if (greaterThan.isEmpty() && atLeast.isEmpty()) {
            return new Version(Integer.MIN_VALUE, Integer.MIN_VALUE);
        }

        checkArgument(
                greaterThan.isEmpty() != atLeast.isEmpty(),
                "You cannot specify 'greaterThan' and 'atLeast' at the same time.");

        if (!greaterThan.isEmpty()) {
            return fromString(greaterThan);
        }

        // For 'atLeast' we need to subtract by one to make it inclusive
        Version v = fromString(atLeast);
        if (v.minor == null) {
            return new Version(v.major - 1, null);
        }
        return new Version(v.major, v.minor - 1);
    }

    /**
     * Parse a version string, with an optional 'v' prefix. Version delimiters might be '.' or '_'.
     *
     * @param versionString a string of a version, e.g. 'V5_1' or '5.0'.
     * @return the {@link Version} that {@code versionString} represents.
     */
    static Version fromString(String versionString) {
        // Trim the 'v' prefix
        if (versionString.toLowerCase(Locale.ROOT).startsWith("v")) {
            versionString = versionString.substring(1);
        }
        if (versionString.equals(KernelVersion.GLORIOUS_FUTURE.name())) {
            return new Version(Integer.MAX_VALUE, Integer.MAX_VALUE);
        }

        String[] components = versionString.split("[._]");
        checkArgument(components.length > 0, "We need at least a major version to compare with");
        int major = Integer.parseInt(components[0]);
        if (components.length < 2) {
            return new Version(major, null);
        }
        int minor = Integer.parseInt(components[1]);

        return new Version(major, minor);
    }

    static Version fromKernelVersion(KernelVersion kernelVersion) {
        return fromString(kernelVersion.name());
    }

    /**
     * @param major version.
     * @param minor version, {@code null} is interpreted as a wildcard.
     */
    record Version(int major, Integer minor) implements Comparable<Version> {
        @Override
        public int compareTo(Version other) {
            if (minor != null && other.minor != null && major == other.major) {
                return Integer.compare(minor, other.minor);
            }
            return Integer.compare(major, other.major);
        }
    }
}
