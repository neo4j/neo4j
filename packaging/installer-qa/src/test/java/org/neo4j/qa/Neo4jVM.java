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
package org.neo4j.qa;

import org.neo4j.vagrant.Box;
import org.neo4j.vagrant.VMDefinition;

public enum Neo4jVM implements VMDefinition {

    // These settings merely reflect the settings
    // in corresponding vagrant configuration.
    // Vagrant config files are in src/test/resources/vagrant
    
    WIN_1(Box.WINDOWS_2008_R2_AMD64, "win2008-1", "33.33.33.10"),
    WIN_2(Box.WINDOWS_2008_R2_AMD64, "win2008-2", "33.33.33.11"),
    WIN_3(Box.WINDOWS_2008_R2_AMD64, "win2008-3", "33.33.33.12"), 
    UBUNTU_1(Box.UBUNTU_11_04_SERVER, "ubuntu-11.04-1", "33.33.33.13"),
    UBUNTU_2(Box.UBUNTU_11_04_SERVER, "ubuntu-11.04-2", "33.33.33.14"),
    UBUNTU_3(Box.UBUNTU_11_04_SERVER, "ubuntu-11.04-3", "33.33.33.15");
    
    private final Box box;
    private final String name;
    private final String ip;
    
    private Neo4jVM(Box box, String name, String ip) {
        this.box = box;
        this.name = name;
        this.ip = ip; 
    }

    @Override
    public String vmName()
    {
        return name;
    }
    
    @Override
    public String ip()
    {
        return ip;
    }
    
    @Override
    public Box box()
    {
        return box;
    }
    
}
