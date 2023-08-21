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

/**
 * System Exit Codes
 * Based off of sysexits.h
 */
public class ExitCode {
    // cannot be enum due to usage in annotations

    public static final int OK = 0; // successful termination
    public static final int FAIL = 1; // generic failure

    public static final int USAGE = 64; // command line usage error
    public static final int DATAERR = 65; // data format error
    public static final int NOINPUT = 66; // cannot open input
    public static final int NOUSER = 67; // addressee unknown
    public static final int NOHOST = 68; // host name unknown
    public static final int UNAVAILABLE = 69; // service unavailable
    public static final int SOFTWARE = 70; // internal software error
    public static final int OSERR = 71; // system error (e.g., can't fork)
    public static final int OSFILE = 72; // critical OS file missing
    public static final int CANTCREAT = 73; // can't create (user) output file
    public static final int IOERR = 74; // input/output error
    public static final int TEMPFAIL = 75; // temp failure; user is invited to retry
    public static final int PROTOCOL = 76; // remote error in protocol
    public static final int NOPERM = 77; // permission denied
    public static final int CONFIG = 78; // configuration error

    private ExitCode() {}
}
