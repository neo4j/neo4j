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
package org.neo4j.qa.machinestate;

public class Neo4jHANodeDescription {

    private MachineModel machine;
    private int id;
    private String ip;

    public Neo4jHANodeDescription(int id, String ip, MachineModel machine)
    {
        this.id = id;
        this.ip = ip;
        this.machine = machine;
    }

    public MachineModel getMachine()
    {
        return machine;
    }

    public int getId()
    {
        return id;
    }

    public String getIp()
    {
        return ip;
    }

}
