/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

public abstract class SimpleBaseCommand implements VagrantCommand<Object> {

    public abstract String arguments();

    @Override
    public Object run(Shell sh, String vagrantPath)
    {
        Result r = sh.run(vagrantPath + " " + arguments());

        if(!verify(r)) {
            throw new VagrantCommandException(getClass().getName(), r);
        }
        
        return null;
    }

    @Override
    public boolean isIdempotent()
    {
        return false;
    }
    
    /**
     * Override in child implementations to add verification that
     * a command has completed successfully. If this is an idempotent
     * command, it will be retried if this fails.
     * 
     * @param r
     * @return
     */
    protected boolean verify(Result r) {
        return r.getExitCode() == 0;
    }
    
}
