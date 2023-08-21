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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;

/**
 * Throw to indicate that detected log version format is not supported.
 */
public class UnsupportedLogVersionException extends RuntimeException {
    private final byte kernelVersion;

    UnsupportedLogVersionException(byte kernelVersion, String message) {
        super(message);
        this.kernelVersion = kernelVersion;
    }

    UnsupportedLogVersionException(byte kernelVersion, String message, Throwable cause) {
        super(message, cause);
        this.kernelVersion = kernelVersion;
    }

    public static UnsupportedLogVersionException unsupported(
            BinarySupportedKernelVersions binarySupportedKernelVersions, byte versionByte) {
        String msg;
        if (binarySupportedKernelVersions.latestSupportedIsLessThan(versionByte)) {
            msg = String.format(
                    "Log file contains entries with prefix %d, and the highest supported Kernel Version is %s. This "
                            + "indicates that the log files originates from an newer version of neo4j, which we don't support "
                            + "downgrading from.",
                    versionByte, binarySupportedKernelVersions);
        } else {
            msg = String.format(
                    "Log file contains entries with prefix %d, and the lowest supported Kernel Version is %s. This "
                            + "indicates that the log files originates from an older version of neo4j, which we don't support "
                            + "migrations from.",
                    versionByte, KernelVersion.EARLIEST);
        }
        return new UnsupportedLogVersionException(versionByte, msg);
    }

    public byte getKernelVersion() {
        return kernelVersion;
    }
}
