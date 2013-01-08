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
package org.neo4j.qa.machinestate;

public class ZookeeperNodeDescription {

    private String ip;
    private int id;
    private int clientPort;
    private MachineModel machine;
    
    public ZookeeperNodeDescription(int id, String ip, int clientPort, MachineModel machine) {
        this.ip = ip;
        this.id = id;
        this.clientPort = clientPort;
        this.machine = machine;
    }

    public int getId()
    {
        return id;
    }

    public String getPeerConnectionURL()
    {
        return ip + ":2888:3888";
    }

    public int getClientPort()
    {
        return clientPort;
    }
    
    public MachineModel getMachine() {
        return machine;
    }

    public String getClientConnectionURL()
    {
        return ip + ":" + getClientPort();
    }

}
