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
package org.neo4j.vagrant;

import java.io.IOException;
import java.security.PublicKey;

import net.schmizz.sshj.SSHClient;
import net.schmizz.sshj.connection.channel.direct.Session;
import net.schmizz.sshj.connection.channel.direct.Session.Command;
import net.schmizz.sshj.transport.TransportException;
import net.schmizz.sshj.transport.verification.HostKeyVerifier;

import org.apache.commons.lang.StringUtils;
import org.neo4j.vagrant.Shell.Result;

public class SSHShell {
    
    private SSHClient client;
    private String vmName;
    private SSHConfig config;
    private static final HostKeyVerifier ALLOW_ALL = new HostKeyVerifier() {
        public boolean verify(String arg0, int arg1, PublicKey arg2) {
            return true;
        }
    };

    public SSHShell(String vmName, SSHConfig config) {
        this.vmName = vmName;
        this.config = config;
        connect();
    }
    
    public Result run(String ... cmds) {
        Session session = null;
        String cmd = StringUtils.join(cmds, " ");
        try {
            session = startSession();
            Shell.logOutput(vmName + " $ ", cmd);
            Command command = session.exec(cmd);
            
            String msg = Shell.outputToString(vmName, command.getInputStream()) + Shell.outputToString(vmName, command.getErrorStream());
            Integer x = command.getExitStatus();
            
            return new Result(x == null? -1 : 0,msg,cmd);
        } catch (Exception e) {
            throw new ShellException(e);
        } finally {
            if(session != null) {
                try {
                    session.close();
                } catch (Exception e) {
                    throw new ShellException(e);
                }
            }
        }
    }
    
    private void connect()
    {
        try {
            if(client != null) {
                try {
                    client.disconnect();
                } catch(AssertionError e) {
                    // om nom nom
                }
            }
            client = new SSHClient();
            client.addHostKeyVerifier(ALLOW_ALL);
            client.connect(config.host(), config.port());
            client.authPublickey(config.user(), client.loadKeys(config.privateKeyPath()));
        } catch(Exception e) {
            throw new ShellException(e);
        }
    }

    public Session startSession() {
        try {
            Session session;
            try {
                session = client.startSession();
            } catch(AssertionError e) {
                connect();
                session = client.startSession();
            } catch(TransportException e) {
                connect();
                session = client.startSession();
            }
            return session;
        } catch (Exception e) {
            throw new ShellException(e);
        }
    }
    
    public void close() {
        try {
            client.close();
        } catch (IOException e) {
            throw new ShellException(e);
        }
    }

}
