/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.tooling;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.tooling.ImportTool.Options;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.tooling.ImportTool.MULTI_FILE_DELIMITER;

public class ImportToolDocIT
{
    private static final int NODE_COUNT = 6;
    private static final int RELATIONSHIP_COUNT = 9;
    private static final int SEQUEL_COUNT = 2;
    public final @Rule
    TestDirectory directory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void basicCsvImport() throws Exception
    {
        // GIVEN
        File movies = file( "ops", "movies.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( ":ID,title,year:int,:LABEL" );
            out.println( "tt0133093,\"The Matrix\",1999,Movie" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003,Movie;Sequel" );
            out.println( "tt0242653,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }

        File actors = file( "ops", "actors.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( ":ID,name,:LABEL" );
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
        }

        File roles = file( "ops", "roles.csv" );
        try (PrintStream out = new PrintStream( roles ))
        {
            out.println( ":START_ID,role,:END_ID,:TYPE" );
            out.println( "keanu,\"Neo\",tt0133093,ACTS_IN" );
            out.println( "keanu,\"Neo\",tt0234215,ACTS_IN" );
            out.println( "keanu,\"Neo\",tt0242653,ACTS_IN" );
            out.println( "laurence,\"Morpheus\",tt0133093,ACTS_IN" );
            out.println( "laurence,\"Morpheus\",tt0234215,ACTS_IN" );
            out.println( "laurence,\"Morpheus\",tt0242653,ACTS_IN" );
            out.println( "carrieanne,\"Trinity\",tt0133093,ACTS_IN" );
            out.println( "carrieanne,\"Trinity\",tt0234215,ACTS_IN" );
            out.println( "carrieanne,\"Trinity\",tt0242653,ACTS_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships", roles.getAbsolutePath() );
        ImportTool.main( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "example-command.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void separateHeadersCsvImport() throws Exception
    {
        // GIVEN
        File moviesHeader = file( "ops", "movies2-header.csv" );
        try ( PrintStream out = new PrintStream( moviesHeader ) )
        {
            out.println( ":ID;title;year:int;:LABEL" );
        }
        File movies = file( "ops", "movies2.csv" );
        try ( PrintStream out = new PrintStream( movies ) )
        {
            out.println( "tt0133093;'The Matrix';1999;Movie" );
            out.println( "tt0234215;'The Matrix Reloaded';2003;Movie|Sequel" );
            out.println( "tt0242653;'The Matrix Revolutions';2003;Movie|Sequel" );
        }
        File actorsHeader = file( "ops", "actors2-header.csv" );
        try ( PrintStream out = new PrintStream( actorsHeader ) )
        {
            out.println( ":ID;name;:LABEL" );
        }
        File actors = file( "ops", "actors2.csv" );
        try ( PrintStream out = new PrintStream( actors ) )
        {
            out.println( "keanu;'Keanu Reeves';Actor" );
            out.println( "laurence;'Laurence Fishburne';Actor" );
            out.println( "carrieanne;'Carrie-Anne Moss';Actor" );
        }
        File rolesHeader = file( "ops", "roles2-header.csv" );
        try ( PrintStream out = new PrintStream( rolesHeader ) )
        {
            out.println( ":START_ID;role;:END_ID;:TYPE" );
        }
        File roles = file( "ops", "roles2.csv" );
        try ( PrintStream out = new PrintStream( roles ) )
        {
            out.println( "keanu;'Neo';tt0133093;ACTS_IN" );
            out.println( "keanu;'Neo';tt0234215;ACTS_IN" );
            out.println( "keanu;'Neo';tt0242653;ACTS_IN" );
            out.println( "laurence;'Morpheus';tt0133093;ACTS_IN" );
            out.println( "laurence;'Morpheus';tt0234215;ACTS_IN" );
            out.println( "laurence;'Morpheus';tt0242653;ACTS_IN" );
            out.println( "carrieanne;'Trinity';tt0133093;ACTS_IN" );
            out.println( "carrieanne;'Trinity';tt0234215;ACTS_IN" );
            out.println( "carrieanne;'Trinity';tt0242653;ACTS_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", moviesHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + movies.getAbsolutePath(),
                "--nodes", actorsHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + actors.getAbsolutePath(),
                "--relationships", rolesHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + roles.getAbsolutePath(),
                "--delimiter", ";",
                "--array-delimiter", "|",
                "--quote", "'");
        ImportTool.main( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "separate-header-example-command.adoc" );

        // THEN
        verifyData();
    }

    private void verifyData()
    {
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = 0, relationshipCount = 0, sequelCount = 0;
            for ( Node node : GlobalGraphOperations.at( db ).getAllNodes() )
            {
                nodeCount++;
            }
            assertEquals( NODE_COUNT, nodeCount );
            for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
            {
                assertTrue( relationship.hasProperty( "role" ) );
                relationshipCount++;
            }
            assertEquals( RELATIONSHIP_COUNT, relationshipCount );
            ResourceIterator<Node> movieSequels = db.findNodes( DynamicLabel.label( "Sequel" ) );
            while ( movieSequels.hasNext() )
            {
                Node sequel = movieSequels.next();
                assertTrue( sequel.hasProperty( "title" ) );
                sequelCount++;
            }
            assertEquals( SEQUEL_COUNT, sequelCount );
            tx.success();
            Object year = db.findNode( DynamicLabel.label( "Movie" ), "title", "The Matrix" ).getProperty( "year" );
            assertEquals( year, 1999 );
        }
        finally
        {
            db.shutdown();
        }
    }

    private void printCommandToFile( String[] arguments, String dir, String fileName ) throws FileNotFoundException
    {
        List<String> cleanedArguments = new ArrayList<>();
        for ( String argument : arguments )
        {
            if ( argument.contains( " " ) || Arrays.asList( new String[] { ";", "|", "'" } ).contains( argument ) )
            {
                cleanedArguments.add( '"' + argument + '"' );
            }
            else
            {
                cleanedArguments.add( argument );
            }
        }
        String documentationArgs = StringUtils.join( cleanedArguments, " " );
        documentationArgs =
                documentationArgs.replace( dir + File.separator, "" ).replace( directory.absolutePath(),
                        "path_to_target_directory" );
        String docsCommand = "neo4j-import " + documentationArgs;
        try ( PrintStream out = new PrintStream( file( "ops", fileName ) ) )
        {
            out.println( docsCommand );
        }
    }

    @Test
    public void printOptionsForManpage() throws Exception
    {
        try (PrintStream out = new PrintStream( file( "man", "options.adoc" ) ))
        {
            for ( Options option : ImportTool.Options.values() )
            {
                out.print( option.manPageEntry() );
            }
        }
    }

    private File file( String section, String name )
    {
        File directory = new File( new File( new File( "target" ), "docs" ), section );
        directory.mkdirs();
        return new File( directory, name );
    }

    private String[] arguments( String... arguments )
    {
        return arguments;
    }
}
