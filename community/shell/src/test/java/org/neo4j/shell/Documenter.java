/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.shell;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.HashMap;
import java.util.Stack;

import org.neo4j.helpers.Exceptions;
import org.neo4j.shell.impl.CollectingOutput;
import org.neo4j.shell.impl.RemoteOutput;
import org.neo4j.shell.impl.SameJvmClient;
import org.neo4j.test.AsciiDocGenerator;

import static org.junit.Assert.*;

public class Documenter
{
    public class DocOutput implements Output, Serializable
    {
        private static final long serialVersionUID = 1L;
        private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        private final PrintWriter out = new PrintWriter( baos );

        @Override
        public Appendable append( final CharSequence csq, final int start, final int end )
        throws IOException
        {
            this.print( RemoteOutput.asString( csq ).substring( start, end ) );
            return this;
        }

        @Override
        public Appendable append( final char c ) throws IOException
        {
            this.print( c );
            return this;
        }

        @Override
        public Appendable append( final CharSequence csq ) throws IOException
        {
            this.print( RemoteOutput.asString( csq ) );
            return this;
        }

        @Override
        public void println( final Serializable object ) throws RemoteException
        {
            out.println( object );
            out.flush();
        }

        @Override
        public void println() throws RemoteException
        {
            out.println();
            out.flush();
        }

        @Override
        public void print( final Serializable object ) throws RemoteException
        {
            out.print( object );
            out.flush();
        }
    }

    public class Job
    {
        public final String query;
        public final String assertion;
        public final String comment;

        public Job( final String query, final String assertion, final String comment )
        {
            this.query = query;
            this.assertion = assertion;
            this.comment = comment;
        }
    }

    private final String title;
    private final Stack<Job> stack = new Stack<Documenter.Job>();
    private final ShellClient client;

    public Documenter( final String title, final ShellServer server )
    {
        this.title = title;
        try
        {
            this.client = new SameJvmClient( new HashMap<String, Serializable>(), server,
                    new CollectingOutput(), InterruptSignalHandler.getHandler() );
        }
        catch ( Exception e )
        {
            throw Exceptions.launderedException( "Error creating client",e );
        }
    }

    public void add( final String query, final String assertion, final String comment )
    {
        stack.push( new Job( query, assertion, comment ) );
    }

    public void run()
    {
        PrintWriter out = getWriter( this.title );
        out.println();
        out.println( "[source, bash]" );
        out.println( "-----" );

        for ( Job job : stack )
        {
            try
            {
                DocOutput output = new DocOutput();
                String prompt = client.getPrompt();
                client.evaluate( job.query, output );
                String result = output.baos.toString();
                assertTrue( result + "did not contain " + job.assertion, result.contains( job.assertion ) );
                doc( job, out, result, prompt );
            }
            catch ( ShellException e )
            {
                throw new RuntimeException( e );
            }
        }
        out.println( "-----" );
        out.flush();
        out.close();
    }

    PrintWriter getWriter( String title )
    {
        return AsciiDocGenerator.getPrintWriter( "target/docs/dev/shell", title );
    }

    private void doc( final Job job, final PrintWriter out, final String result, String prompt )
    {
        if ( job.query != null && !job.query.isEmpty() )
        {
            out.println( " # " + job.comment );
            out.println( " " + prompt + job.query );
            if ( result != null && !result.isEmpty() )
            {
                out.println( " " + result.replace( "\n", "\n " ) );
            }
            out.println();
        }
    }
}
