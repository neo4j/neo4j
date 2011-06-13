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
package org.neo4j.shell;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.Stack;

public class Documenter
{

    public class DocOutput implements Output, Serializable
    {
        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        public ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintWriter out = new PrintWriter( baos );

        @Override
        public Appendable append( CharSequence csq, int start, int end )
                throws IOException
        {
            return null;
        }

        @Override
        public Appendable append( char c ) throws IOException
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public Appendable append( CharSequence csq ) throws IOException
        {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public void println( Serializable object ) throws RemoteException
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
        public void print( Serializable object ) throws RemoteException
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

        public Job( String query, String assertion, String comment )
        {
            this.query = query;
            this.assertion = assertion;
            this.comment = comment;
        }

    }

    private final String title;
    private final Stack<Job> stack = new Stack<Documenter.Job>();
    private final ShellClient client;

    public Documenter( String title, ShellClient client )
    {
        this.title = title;
        this.client = client;

    }

    public void add( String query, String assertion, String comment )
    {
        stack.push( new Job( query, assertion, comment ) );
    }

    public void run()
    {
        File dir = new File( "target/docs/dev/shell" );
        if ( !dir.exists() )
        {
            dir.mkdirs();
        }
        File file = new File( dir, this.title.toLowerCase().replace( " ", "-" )
                                   + ".txt" );
        PrintWriter out = null;
        try
        {
            out = new PrintWriter( new FileWriter( file ) );
        }
        catch ( IOException e1 )
        {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        out.println("[source,plain]");
        out.println("-----");
        for ( Job job : stack )
        {
            try
            {
                DocOutput output = new DocOutput();
                client.getServer().interpretLine( job.query, client.session(),
                        output );
                String result = output.baos.toString();
                assertTrue( result + "did not contain " + job.assertion, result.contains( job.assertion ) );
                doc( job, out, result );
            }
            catch ( RemoteException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            catch ( ShellException e )
            {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        out.println("-----");
        out.flush();
        out.close();

    }

    private void doc( Job job, PrintWriter out, String result )
    {
            out.println( " # " + job.comment );
            out.println( " " + job.query );
            out.println( " " + result.replace( "\n", "\n " ) );
    }
}
