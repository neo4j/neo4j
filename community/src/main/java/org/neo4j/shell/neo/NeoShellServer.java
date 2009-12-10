/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.neo;

import java.io.Serializable;
import java.rmi.RemoteException;

import org.neo4j.api.core.EmbeddedReadOnlyNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Transaction;
import org.neo4j.shell.AbstractClient;
import org.neo4j.shell.BashVariableInterpreter;
import org.neo4j.shell.Session;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellServer;
import org.neo4j.shell.SimpleAppServer;
import org.neo4j.shell.BashVariableInterpreter.Replacer;
import org.neo4j.shell.apps.Help;
import org.neo4j.shell.neo.apps.Cd;
import org.neo4j.shell.neo.apps.Gsh;
import org.neo4j.shell.neo.apps.Jsh;
import org.neo4j.shell.neo.apps.Ls;
import org.neo4j.shell.neo.apps.Mkrel;
import org.neo4j.shell.neo.apps.Mv;
import org.neo4j.shell.neo.apps.NeoApp;
import org.neo4j.shell.neo.apps.Pwd;
import org.neo4j.shell.neo.apps.Rm;
import org.neo4j.shell.neo.apps.Rmrel;
import org.neo4j.shell.neo.apps.Set;
import org.neo4j.shell.neo.apps.Trav;

/**
 * A {@link ShellServer} which contains common methods to use with neo.
 */
public class NeoShellServer extends SimpleAppServer
{
    private NeoService neo;
    private BashVariableInterpreter bashInterpreter;

    /**
     * @param neo
     *            the {@link NeoService} instance to use with the shell server.
     * @throws RemoteException
     *             if an RMI error occurs.
     */
    public NeoShellServer( NeoService neo ) throws RemoteException
    {
        super();
        addNeoApps();
        this.neo = neo;
        this.bashInterpreter = new BashVariableInterpreter();
        this.bashInterpreter.addReplacer( "W", new WorkingDirReplacer() );
        this.setProperty( AbstractClient.PROMPT_KEY, getNeoShellPrompt() );
        this.setProperty( AbstractClient.TITLE_KEYS_KEY,
            ".*name.*,.*title.*" );
        this.setProperty( AbstractClient.TITLE_MAX_LENGTH, "40" );
    }
    
    protected String getNeoShellPrompt()
    {
        String name = "neo-sh";
        if ( this.neo instanceof EmbeddedReadOnlyNeo )
        {
            name += "[readonly]";
        }
        name += " \\W$ ";
        return name;
    }

    @Override
    public String welcome()
    {
        return "Welcome to NeoShell\n" + Help.getHelpString( this );
    }
    
    private void addNeoApps()
    {
        addApp( Cd.class );
        addApp( Gsh.class );
        addApp( Jsh.class );
        addApp( Ls.class );
        addApp( Mkrel.class );
        addApp( Mv.class );
        addApp( Pwd.class );
        addApp( Rm.class );
        addApp( Rmrel.class );
        addApp( Set.class );
        addApp( Trav.class );
    }

    @Override
    public Serializable interpretVariable( String key, Serializable value,
        Session session ) throws ShellException
    {
        Transaction tx = getNeo().beginTx();
        try
        {
            Serializable result = value;
            if ( key.equals( AbstractClient.PROMPT_KEY ) )
            {
                result = this.bashInterpreter.interpret( (String) value, this,
                    session );
            }
            tx.success();
            return result;
        }
        finally
        {
            tx.finish();
        }
    }

    /**
     * @return the {@link NeoService} instance given in the constructor.
     */
    public NeoService getNeo()
    {
        return this.neo;
    }

    /**
     * A {@link Replacer} for the variable "w"/"W" which returns the current
     * working directory (Bash), i.e. the current node.
     */
    public static class WorkingDirReplacer implements Replacer
    {
        public String getReplacement( ShellServer server, Session session )
            throws ShellException
        {
            return NeoApp.getDisplayName( ( NeoShellServer ) server, session,
                NeoApp.getCurrent( ( NeoShellServer ) server,
                    session ) ).toString();
        }
    }
}