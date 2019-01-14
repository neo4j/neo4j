/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.dbms.diagnostics.jmx;

import com.sun.tools.attach.AttachNotSupportedException;
import com.sun.tools.attach.VirtualMachine;

import java.io.IOException;
import java.util.Properties;

public class LocalVirtualMachine
{
    private final String address;
    private final Properties systemProperties;

    private LocalVirtualMachine( String address, Properties systemProperties )
    {
        this.address = address;
        this.systemProperties = systemProperties;
    }

    public String getJmxAddress()
    {
        return address;
    }

    public Properties getSystemProperties()
    {
        return systemProperties;
    }

    /**
     * Get an instance from a process id and makes sure the a JMX agent is running on it.
     *
     * @param pid process id of the jvm to attach to.
     * @return a virtual machine with a JMX endpoint available.
     * @throws IOException if any operations failed.
     */
    public static LocalVirtualMachine from( long pid ) throws IOException
    {
        VirtualMachine vm = null;
        try
        {
            // Try to attach to instance
            vm = VirtualMachine.attach( String.valueOf( pid ) );

            // Get local jmx address if management agent is already started
            Properties agentProps = vm.getAgentProperties();
            String address = (String) agentProps.get( "com.sun.management.jmxremote.localConnectorAddress" );

            // Failed, we are the first one connecting, start agent
            if ( address == null )
            {
                address = vm.startLocalManagementAgent();
            }

            return new LocalVirtualMachine( address, vm.getSystemProperties() );
        }
        catch ( AttachNotSupportedException x )
        {
            throw new IOException( x.getMessage(), x );
        }
        catch ( Exception e )
        {
            // ibm jdk uses a separate exception
            if ( e.getClass().getCanonicalName().equals( "com.ibm.tools.attach.AttachNotSupportedException" ) )
            {
                throw new IOException( e );
            }
            throw e;
        }
        finally
        {
            if ( vm != null )
            {
                vm.detach();
            }
        }
    }
}
