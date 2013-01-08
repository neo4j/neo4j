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

import java.util.HashMap;
import java.util.Map;

import org.neo4j.vagrant.SSHConfig;
import org.neo4j.vagrant.Shell;
import org.neo4j.vagrant.Shell.Result;

public class GetSshConfig implements VagrantCommand<SSHConfig> {

    @Override
    public SSHConfig run(Shell sh, String vagrantPath)
    {
        Result r = sh.run(vagrantPath, "ssh-config");
        return createSshConfigFromOutput(r);
    }

    @Override
    public boolean isIdempotent()
    {
        return true;
    }
    
    private SSHConfig createSshConfigFromOutput(Result r)
    {
        Map<String,String> cfg = new HashMap<String,String>();
        for(String line : r.getOutputAsList()) {
            if(line.startsWith("  ")) {
                String[] split = line.substring(2).split(" ");
                cfg.put(split[0].toLowerCase(), split[1]);
            }
        }
        
        Integer port = Integer.parseInt(cfg.get("port"));
        String host = cfg.get("hostname");
        String privateKeyPath = cfg.get("identityfile");
        String user = cfg.get("user"); 
        
        return new SSHConfig(user, privateKeyPath, host, port);
    }

}
