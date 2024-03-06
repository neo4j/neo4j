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
package org.neo4j.cli;

import static java.util.Objects.requireNonNull;

import java.io.InputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.util.VisibleForTesting;

public class ExecutionContext {
    private final Path homeDir;
    private final Path confDir;
    private final PrintStream out;
    private final PrintStream err;
    private final InputStream in;
    private final FileSystemAbstraction fs;

    public ExecutionContext(Path homeDir, Path confDir) {
        this(homeDir, confDir, new DefaultFileSystemAbstraction());
    }

    @VisibleForTesting
    public ExecutionContext(Path homeDir, Path confDir, FileSystemAbstraction fs) {
        this(homeDir, confDir, System.out, System.err, fs);
    }

    @VisibleForTesting
    public ExecutionContext(Path homeDir, Path confDir, PrintStream out, PrintStream err, FileSystemAbstraction fs) {
        this(homeDir, confDir, out, err, System.in, fs);
    }

    public ExecutionContext(
            Path homeDir, Path confDir, PrintStream out, PrintStream err, InputStream in, FileSystemAbstraction fs) {
        this.homeDir = requireNonNull(homeDir);
        this.confDir = requireNonNull(confDir);
        this.out = requireNonNull(out);
        this.err = requireNonNull(err);
        this.in = requireNonNull(in);
        this.fs = requireNonNull(fs);
    }

    public PrintStream out() {
        return out;
    }

    public PrintStream err() {
        return err;
    }

    public InputStream in() {
        return in;
    }

    public FileSystemAbstraction fs() {
        return fs;
    }

    public Path homeDir() {
        return homeDir;
    }

    public Path confDir() {
        return confDir;
    }
}
