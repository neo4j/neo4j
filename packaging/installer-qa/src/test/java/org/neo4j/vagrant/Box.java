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

public enum Box {
    
    WINDOWS_2008_R2_AMD64("windows-2008R2-amd64", 
            System.getProperty("box-path-win2008-amd64")),
            
    UBUNTU_11_04_SERVER("ubuntu-11.04-amd64-with-jre", 
            System.getProperty("box-path-ubuntu-11.04-amd64-with-jre"));
    
    private String boxName;
    private String boxUrl;

    private Box(String boxName, String boxUrl) {
        this.boxName = boxName;
        this.boxUrl = boxUrl;
    }

    public String getName() {
        return boxName;
    }

    public String getUrl() {
        return boxUrl;
    }
    
}
