/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.vagrant;

import org.neo4j.vagrant.Shell.Result;

public class ShellException extends RuntimeException {

    /**
     * 
     */
    private static final long serialVersionUID = -1718095011821914612L;

    private Result result;

    public ShellException(Result r)
    {
        super("'" + r.getCommand() + "' failed with exit code: " + r.getExitCode());
        result = r;
    }

    public ShellException(Throwable e)
    {
        super(e);
    }

    public Result result()
    {
        return result;
    }

}
