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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Facts about a single log file, viewed from a pruning decision's point of view. Instances are constructed per
 * visited version during a pruning cycle and live only for that visit.
 */
public interface LogFileInformation {
    long version();

    Path path();

    /** Last append index recorded in this file's header (i.e. the previous file's last append index). */
    long getPreviousAppendIndexFromHeader() throws IOException;

    /** Timestamp of the first start record in this file. */
    long getFirstStartRecordTimestamp() throws IOException;
}
