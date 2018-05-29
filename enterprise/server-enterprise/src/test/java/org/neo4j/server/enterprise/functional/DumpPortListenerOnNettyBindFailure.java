/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise.functional;

import org.apache.commons.lang3.SystemUtils;
import org.jboss.netty.channel.ChannelException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.Writer;

public class DumpPortListenerOnNettyBindFailure implements TestRule
{
    private final PrintWriter out;

    public DumpPortListenerOnNettyBindFailure()
    {
        this(System.err);
    }

    public DumpPortListenerOnNettyBindFailure( OutputStream out )
    {
        this( new PrintWriter( out, true ) );
    }

    public DumpPortListenerOnNettyBindFailure( Writer out )
    {
        this.out = out instanceof PrintWriter ? (PrintWriter) out : new PrintWriter( out, true );
    }

    @Override
    public Statement apply( final Statement base, Description description )
    {
        if ( SystemUtils.IS_OS_WINDOWS )
        {
            return base; // we don't have 'lsof' on windows...
        }
        return new Statement()
        {
            @Override
            public void evaluate() throws Throwable
            {
                try
                {
                    base.evaluate();
                }
                catch ( Throwable failure )
                {
                    for ( Throwable cause = failure; cause != null; cause = cause.getCause() )
                    {
                        if ( cause instanceof ChannelException )
                        {
                            String message = cause.getMessage();
                            String portString = message.substring( message.lastIndexOf( ':' ) + 1 );
                            int port;
                            try
                            {
                                port = Integer.parseInt( portString );
                            }
                            catch ( Exception e )
                            {
                                continue;
                            }
                            dumpListenersOn( port );
                        }
                    }
                    throw failure;
                }
            }
        };
    }

    public void dumpListenersOn( int port )
    {
        try
        {
            Process lsof = new ProcessBuilder( "lsof", "-i:" + port ).redirectErrorStream( true ).start();
            lsof.waitFor();
            out.println( readAll( lsof.getInputStream() ) );
        }
        catch ( Exception e )
        {
            out.println( "Could not determine the process listening on :" + port );
        }
    }

    private static String readAll( InputStream stream ) throws IOException
    {
        StringBuilder message = new StringBuilder();
        BufferedReader err = new BufferedReader( new InputStreamReader( stream ) );
        for ( String line; null != (line = err.readLine()); )
        {
            message.append( line ).append( '\n' );
        }
        err.close();
        return message.toString();
    }
}
