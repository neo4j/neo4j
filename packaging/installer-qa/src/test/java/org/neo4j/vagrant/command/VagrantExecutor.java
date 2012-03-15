/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.vagrant.command;

import org.neo4j.vagrant.Shell;

public class VagrantExecutor {

    private String vagrantPath;
    private Shell sh;
    private int maxRetries;

    public VagrantExecutor(Shell sh, String vagrantPath, int maxRetries)
    {
        this.sh = sh;
        this.vagrantPath = vagrantPath;
        this.maxRetries = maxRetries;
    }

    public <T> T execute(VagrantCommand<T> cmd)
    {
        if (cmd.isIdempotent())
        {
            int retries = 0;
            RuntimeException exception = null;

            while (retries < maxRetries)
            {
                retries++;
                try
                {
                    return cmd.run(sh, vagrantPath);
                } catch (RuntimeException e)
                {
                    exception = e;
                }
            }

            throw exception;

        } else
        {
            return cmd.run(sh, vagrantPath);
        }
    }

}
