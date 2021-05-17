/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.startup;

import static org.neo4j.server.startup.ProcessManager.behaviour;

abstract class AbstractUnixBootloaderOs extends BootloaderOsAbstraction
{
    AbstractUnixBootloaderOs( BootloaderContext ctx )
    {
        super( ctx );
    }

    @Override
    long start()
    {
        return ctx.processManager().run( buildStandardStartArguments(), behaviour().redirectToUserLog().storePid() );
    }

    @Override
    void stop( long pid ) throws BootFailureException
    {
        ProcessHandle process = getProcessIfAlive( pid );
        if ( process != null )
        {
            process.destroy();
        }
    }

    @Override
    protected ProcessManager.Behaviour consoleBehaviour()
    {
        return super.consoleBehaviour().tryStorePid();
    }

    @Override
    void installService() throws BootFailureException
    {
        throw new UnsupportedOperationException( "Not supported on this OS" );
    }

    @Override
    void uninstallService() throws BootFailureException
    {
        throw new UnsupportedOperationException( "Not supported on this OS" );
    }

    @Override
    void updateService() throws BootFailureException
    {
        throw new UnsupportedOperationException( "Not supported on this OS" );
    }

    @Override
    boolean serviceInstalled()
    {
        throw new UnsupportedOperationException( "Not supported on this OS" );
    }

    @Override
    Long getPidIfRunning()
    {
        ProcessHandle handle = getProcessIfAlive( ctx.processManager().getPidFromFile() );
        return handle != null ? handle.pid() : null;
    }

    private ProcessHandle getProcessIfAlive( Long pid )
    {
        if ( pid != null )
        {
            ProcessHandle process = ctx.processManager().getProcessHandle( pid );
            return process != null && process.isAlive() ? process : null;
        }
        return null;
    }
}
