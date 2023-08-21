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
package org.neo4j.kernel.impl.locking.forseti;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import org.neo4j.kernel.impl.locking.DumpLocksVisitor;
import org.neo4j.kernel.impl.locking.LockManager;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.log4j.Log4jLogProvider;

public class LockWorkFailureDump {
    private final Path file;

    public LockWorkFailureDump(Path file) {
        this.file = file;
    }

    public Path dumpState(LockManager lm, LockWorker... workers) throws IOException {
        try (OutputStream out = Files.newOutputStream(file)) {
            InternalLogProvider logProvider = new Log4jLogProvider(out);
            //  * locks held by the lock manager
            lm.accept(new DumpLocksVisitor(logProvider.getLog(LockWorkFailureDump.class)));
            //  * rag manager state;
            //  * workers state
            InternalLog log = logProvider.getLog(getClass());
            for (LockWorker worker : workers) {
                // - what each is doing and have up to now
                log.info("Worker %s", worker);
            }
            return file;
        }
    }
}
