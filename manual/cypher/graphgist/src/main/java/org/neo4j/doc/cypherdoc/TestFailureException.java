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
package org.neo4j.doc.cypherdoc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

class TestFailureException extends RuntimeException
{
    final Result result;
    private List<Snapshot> snapshots = new ArrayList<>();

    TestFailureException( Result result, List<String> failedTests )
    {
        super( message( failedTests ) );
        this.result = result;
    }

    private static String message( List<String> failedTests )
    {
        StringBuilder message = new StringBuilder( "Query validation failed:" );
        for ( String test : failedTests )
        {
            message.append( CypherDoc.EOL )
                   .append( "\tQuery result doesn't contain the string '" ).append( test ).append( "'." );
        }
        return message.toString();
    }

    @Override
    public String toString()
    {
        StringBuilder message = new StringBuilder( getMessage() );
        message.append( CypherDoc.EOL )
               .append( "Query:" ).append( CypherDoc.EOL ).append( '\t' )
               .append( CypherDoc.indent( result.query ) );
        message.append( CypherDoc.EOL )
               .append( "Result:" ).append( CypherDoc.EOL ).append( '\t' )
               .append( CypherDoc.indent( result.text ) );
        message.append( CypherDoc.EOL )
               .append( "Profile:" ).append( CypherDoc.EOL ).append( '\t' )
               .append( CypherDoc.indent( result.profile ) );
        if ( !snapshots.isEmpty() )
        {
            message.append( CypherDoc.EOL ).append( "Snapshots:" );
            for ( Snapshot snapshot : snapshots )
            {
                message.append( CypherDoc.EOL ).append( '\t' ).append( snapshot );
            }
        }
        return message.toString();
    }

    synchronized void addSnapshot( String key, byte[] bytes )
    {
        snapshots.add( new InMemorySnapshot( key, bytes ) );
    }

    synchronized void dumpSnapshots( File targetDir )
    {
        List<Snapshot> prior = snapshots;
        snapshots = new ArrayList<>( prior.size() );
        for ( Snapshot snapshot : prior )
        {
            snapshots.add( snapshot.dump( targetDir ) );
        }
    }

    private static abstract class Snapshot
    {
        final String filename;

        Snapshot( String filename )
        {
            this.filename = filename;
        }

        @Override
        public String toString()
        {
            return getClass().getSimpleName() + "[" + filename + "]";
        }

        abstract Snapshot dump( File targetDir );
    }

    private static class InMemorySnapshot extends Snapshot
    {
        private final byte[] bytes;

        InMemorySnapshot( String filename, byte[] bytes )
        {
            super( filename );
            this.bytes = bytes;
        }

        @Override
        public Snapshot dump( File targetDir )
        {
            File target = new File( targetDir, filename );
            try ( FileOutputStream output = new FileOutputStream( target ) )
            {
                output.write( bytes );
                return new DumpedSnapshot( target.getAbsolutePath() );
            }
            catch ( IOException e )
            {
                return this;
            }
        }
    }

    private static class DumpedSnapshot extends Snapshot
    {
        public DumpedSnapshot( String path )
        {
            super( path );
        }

        @Override
        public Snapshot dump( File targetDir )
        {
            return this;
        }
    }
}
