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
package org.neo4j.vagrant.command;

import org.neo4j.vagrant.Shell;
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.command.GetState.VirtualMachineState;

public class Halt implements VagrantCommand<Object> {

    private static final String HALT = "halt";

    @Override
    public Object run(Shell sh, String vagrantPath)
    {
        Result haltResult = sh.run(vagrantPath + " " + HALT);

        if (!verify(haltResult))
        {
            GetState status = new GetState();
            if (status.run(sh, vagrantPath) != VirtualMachineState.POWEROFF)
            {
                throw new VagrantCommandException(getClass().getName(),
                        haltResult);
            }
        }

        return null;
    }

    @Override
    public boolean isIdempotent()
    {
        return true;
    }

    protected boolean verify(Result r)
    {
        return r.getOutput()
                .endsWith("Attempting graceful shutdown of VM...\n");
    }

}
