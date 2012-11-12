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
package org.neo4j.vagrant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SSHConfig {

    public static SSHConfig createFromVagrantOutput(List<String> out) {
        Map<String,String> cfg = new HashMap<String,String>();
        for(String line : out) {
            if(line.startsWith("  ")) {
                String[] split = line.substring(2).split(" ");
                cfg.put(split[0].toLowerCase(), split[1]);
            }
        }
        
        SSHConfig config = new SSHConfig();
        
        config.port = Integer.parseInt(cfg.get("port"));
        config.host = cfg.get("hostname");
        config.privateKeyPath = cfg.get("identityfile");
        config.user = cfg.get("user"); 
        
        return config;
    }

    private String privateKeyPath;
    private String user;
    private String host;
    private int port;

    public int port()
    {
        return port;
    }

    public String host()
    {
        return host;
    }

    public String user()
    {
        return user;
    }

    public String privateKeyPath()
    {
        return privateKeyPath;
    }
    
}
