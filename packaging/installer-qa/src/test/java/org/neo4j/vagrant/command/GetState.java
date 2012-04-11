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
import org.neo4j.vagrant.Shell.Result;
import org.neo4j.vagrant.command.GetState.VirtualMachineState;

public class GetState implements VagrantCommand<VirtualMachineState> {

    private static final String STATUS = "status";

    public enum VirtualMachineState {
        POWEROFF, RUNNING, NOT_CREATED, SAVED
    }

    @Override
    public VirtualMachineState run(Shell sh, String vagrantPath)
    {
        Result statusResult = sh.run(vagrantPath + " " + STATUS);
        String output = statusResult.getOutput();

        if (output.contains("running\n"))
        {
            return VirtualMachineState.RUNNING;
        } else if (output.contains("poweroff\n"))
        {
            return VirtualMachineState.POWEROFF;
        } else if (output.contains("not created\n"))
        {
            return VirtualMachineState.NOT_CREATED;
        } else if (output.contains("saved\n"))
        {
            return VirtualMachineState.SAVED;
        }

        throw new VagrantCommandException(getClass().getName(), statusResult,
                "Did not recognize VM state.");
    }

    @Override
    public boolean isIdempotent()
    {
        return true;
    }

}
