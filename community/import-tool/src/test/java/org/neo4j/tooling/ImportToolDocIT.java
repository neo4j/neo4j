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
package org.neo4j.tooling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.Charsets;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.tooling.ImportTool.Options;
import org.neo4j.unsafe.impl.batchimport.Configuration;

import static org.neo4j.helpers.ArrayUtil.join;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.count;
import static org.neo4j.io.fs.FileUtils.readTextFile;
import static org.neo4j.io.fs.FileUtils.writeToFile;
import static org.neo4j.tooling.GlobalGraphOperations.at;
import static org.neo4j.tooling.ImportTool.MULTI_FILE_DELIMITER;

import org.apache.commons.lang3.StringUtils;
import org.junit.Rule;
import org.junit.Test;

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
            out.println( "movieId:ID,title,year:int,:LABEL" );
            out.println( "tt0133093,\"The Matrix\",1999,Movie" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003,Movie;Sequel" );
            out.println( "tt0242653,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }

        File actors = file( "ops", "actors.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID,name,:LABEL" );
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
        }

        File roles = file( "ops", "roles.csv" );
        try (PrintStream out = new PrintStream( roles ))
        {
            out.println( ":START_ID,role,:END_ID,:TYPE" );
            out.println( "keanu,\"Neo\",tt0133093,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0234215,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0242653,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0133093,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0234215,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0242653,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0133093,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0234215,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0242653,ACTED_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships", roles.getAbsolutePath() );
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "example-command.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void basicCsvImportCustomDelimiterQuotation() throws Exception
    {
        // GIVEN
        File movies = file( "ops", "movies2.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( "movieId:ID;title;year:int;:LABEL" );
            out.println( "tt0133093;'The Matrix';1999;Movie" );
            out.println( "tt0234215;'The Matrix Reloaded';2003;Movie|Sequel" );
            out.println( "tt0242653;'The Matrix Revolutions';2003;Movie|Sequel" );
        }

        File actors = file( "ops", "actors2.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID;name;:LABEL" );
            out.println( "keanu;'Keanu Reeves';Actor" );
            out.println( "laurence;'Laurence Fishburne';Actor" );
            out.println( "carrieanne;'Carrie-Anne Moss';Actor" );
        }

        File roles = file( "ops", "roles2.csv" );
        try (PrintStream out = new PrintStream( roles ))
        {
            out.println( ":START_ID;role;:END_ID;:TYPE" );
            out.println( "keanu;'Neo';tt0133093;ACTED_IN" );
            out.println( "keanu;'Neo';tt0234215;ACTED_IN" );
            out.println( "keanu;'Neo';tt0242653;ACTED_IN" );
            out.println( "laurence;'Morpheus';tt0133093;ACTED_IN" );
            out.println( "laurence;'Morpheus';tt0234215;ACTED_IN" );
            out.println( "laurence;'Morpheus';tt0242653;ACTED_IN" );
            out.println( "carrieanne;'Trinity';tt0133093;ACTED_IN" );
            out.println( "carrieanne;'Trinity';tt0234215;ACTED_IN" );
            out.println( "carrieanne;'Trinity';tt0242653;ACTED_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships", roles.getAbsolutePath(),
                "--delimiter", ";",
                "--array-delimiter", "|",
                "--quote", "'");
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "custom-delimiter-quotation-command.adoc" );

        // THEN
        verifyData();
    }


    @Test
    public void separateHeadersCsvImport() throws Exception
    {
        // GIVEN
        File moviesHeader = file( "ops", "movies3-header.csv" );
        try ( PrintStream out = new PrintStream( moviesHeader ) )
        {
            out.println( "movieId:ID,title,year:int,:LABEL" );
        }
        File movies = file( "ops", "movies3.csv" );
        try ( PrintStream out = new PrintStream( movies ) )
        {
            out.println( "tt0133093,\"The Matrix\",1999,Movie" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003,Movie;Sequel" );
            out.println( "tt0242653,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }
        File actorsHeader = file( "ops", "actors3-header.csv" );
        try ( PrintStream out = new PrintStream( actorsHeader ) )
        {
            out.println( "personId:ID,name,:LABEL" );
        }
        File actors = file( "ops", "actors3.csv" );
        try ( PrintStream out = new PrintStream( actors ) )
        {
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
        }
        File rolesHeader = file( "ops", "roles3-header.csv" );
        try ( PrintStream out = new PrintStream( rolesHeader ) )
        {
            out.println( ":START_ID,role,:END_ID,:TYPE" );
        }
        File roles = file( "ops", "roles3.csv" );
        try ( PrintStream out = new PrintStream( roles ) )
        {
            out.println( "keanu,\"Neo\",tt0133093,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0234215,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0242653,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0133093,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0234215,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0242653,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0133093,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0234215,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0242653,ACTED_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", moviesHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + movies.getAbsolutePath(),
                "--nodes", actorsHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + actors.getAbsolutePath(),
                "--relationships", rolesHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + roles.getAbsolutePath());
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "separate-header-example-command.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void multipleInputFiles() throws Exception
    {
        // GIVEN
        File moviesHeader = file( "ops", "movies4-header.csv" );
        try ( PrintStream out = new PrintStream( moviesHeader ) )
        {
            out.println( "movieId:ID,title,year:int,:LABEL" );
        }
        File moviesPart1 = file( "ops", "movies4-part1.csv" );
        try ( PrintStream out = new PrintStream( moviesPart1 ) )
        {
            out.println( "tt0133093,\"The Matrix\",1999,Movie" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003,Movie;Sequel" );
        }
        File moviesPart2 = file( "ops", "movies4-part2.csv" );
        try ( PrintStream out = new PrintStream( moviesPart2 ) )
        {
            out.println( "tt0242653,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }
        File actorsHeader = file( "ops", "actors4-header.csv" );
        try ( PrintStream out = new PrintStream( actorsHeader ) )
        {
            out.println( "personId:ID,name,:LABEL" );
        }
        File actorsPart1 = file( "ops", "actors4-part1.csv" );
        try ( PrintStream out = new PrintStream( actorsPart1 ) )
        {
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
        }
        File actorsPart2 = file( "ops", "actors4-part2.csv" );
        try ( PrintStream out = new PrintStream( actorsPart2 ) )
        {
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
        }
        File rolesHeader = file( "ops", "roles4-header.csv" );
        try ( PrintStream out = new PrintStream( rolesHeader ) )
        {
            out.println( ":START_ID,role,:END_ID,:TYPE" );
        }
        File rolesPart1 = file( "ops", "roles4-part1.csv" );
        try ( PrintStream out = new PrintStream( rolesPart1 ) )
        {
            out.println( "keanu,\"Neo\",tt0133093,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0234215,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0242653,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0133093,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0234215,ACTED_IN" );
        }
        File rolesPart2 = file( "ops", "roles4-part2.csv" );
        try ( PrintStream out = new PrintStream( rolesPart2 ) )
        {
            out.println( "laurence,\"Morpheus\",tt0242653,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0133093,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0234215,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0242653,ACTED_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", moviesHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + moviesPart1.getAbsolutePath() +
                        MULTI_FILE_DELIMITER + moviesPart2.getAbsolutePath(),
                "--nodes", actorsHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + actorsPart1.getAbsolutePath() +
                        MULTI_FILE_DELIMITER + actorsPart2.getAbsolutePath(),
                "--relationships", rolesHeader.getAbsolutePath() + MULTI_FILE_DELIMITER + rolesPart1.getAbsolutePath() +
                        MULTI_FILE_DELIMITER + rolesPart2.getAbsolutePath() );
        importTool( arguments );

        // DOCS
        String realDir = moviesPart2.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "multiple-input-files.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void sameNodeLabelEverywhere() throws Exception
    {
        // GIVEN
        File movies = file( "ops", "movies5.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( "movieId:ID,title,year:int" );
            out.println( "tt0133093,\"The Matrix\",1999" );
        }

        File sequels = file( "ops", "sequels5.csv" );
        try (PrintStream out = new PrintStream( sequels ))
        {
            out.println( "movieId:ID,title,year:int" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003" );
            out.println( "tt0242653,\"The Matrix Revolutions\",2003" );
        }

        File actors = file( "ops", "actors5.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID,name" );
            out.println( "keanu,\"Keanu Reeves\"" );
            out.println( "laurence,\"Laurence Fishburne\"" );
            out.println( "carrieanne,\"Carrie-Anne Moss\"" );
        }

        File roles = file( "ops", "roles5.csv" );
        try ( PrintStream out = new PrintStream( roles ) )
        {
            out.println( ":START_ID,role,:END_ID,:TYPE" );
            out.println( "keanu,\"Neo\",tt0133093,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0234215,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0242653,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0133093,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0234215,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0242653,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0133093,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0234215,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0242653,ACTED_IN" );
        }
        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes:" + join( new String[] { "Movie" }, ":" ),
                    movies.getAbsolutePath(),
                "--nodes:" + join( new String[] { "Movie", "Sequel" }, ":" ),
                    sequels.getAbsolutePath(),
                "--nodes:" + join( new String[] { "Actor" }, ":" ),
                    actors.getAbsolutePath(),
                "--relationships", roles.getAbsolutePath() );
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "same-node-label-everywhere.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void sameRelationshipTypeEverywhere() throws Exception
    {
        // GIVEN
        File movies = file( "ops", "movies6.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( "movieId:ID,title,year:int,:LABEL" );
            out.println( "tt0133093,\"The Matrix\",1999,Movie" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003,Movie;Sequel" );
            out.println( "tt0242653,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }

        File actors = file( "ops", "actors6.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID,name,:LABEL" );
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
        }

        File roles = file( "ops", "roles6.csv" );
        try ( PrintStream out = new PrintStream( roles ) )
        {
            out.println( ":START_ID,role,:END_ID" );
            out.println( "keanu,\"Neo\",tt0133093" );
            out.println( "keanu,\"Neo\",tt0234215" );
            out.println( "keanu,\"Neo\",tt0242653" );
            out.println( "laurence,\"Morpheus\",tt0133093" );
            out.println( "laurence,\"Morpheus\",tt0234215" );
            out.println( "laurence,\"Morpheus\",tt0242653" );
            out.println( "carrieanne,\"Trinity\",tt0133093" );
            out.println( "carrieanne,\"Trinity\",tt0234215" );
            out.println( "carrieanne,\"Trinity\",tt0242653" );
        }
        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships:" + join( new String[] { "ACTED_IN" }, ":" ), roles.getAbsolutePath());
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "same-relationship-type-everywhere.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void idSpaces() throws Exception
    {
        // GIVEN
        File movies = file( "ops", "movies8.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( "movieId:ID(Movie),title,year:int,:LABEL" );
            out.println( "1,\"The Matrix\",1999,Movie" );
            out.println( "2,\"The Matrix Reloaded\",2003,Movie;Sequel" );
            out.println( "3,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }

        File actors = file( "ops", "actors8.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID(Actor),name,:LABEL" );
            out.println( "1,\"Keanu Reeves\",Actor" );
            out.println( "2,\"Laurence Fishburne\",Actor" );
            out.println( "3,\"Carrie-Anne Moss\",Actor" );
        }

        File roles = file( "ops", "roles8.csv" );
        try ( PrintStream out = new PrintStream( roles ) )
        {
            out.println( ":START_ID(Actor),role,:END_ID(Movie)" );
            out.println( "1,\"Neo\",1" );
            out.println( "1,\"Neo\",2" );
            out.println( "1,\"Neo\",3" );
            out.println( "2,\"Morpheus\",1" );
            out.println( "2,\"Morpheus\",2" );
            out.println( "2,\"Morpheus\",3" );
            out.println( "3,\"Trinity\",1" );
            out.println( "3,\"Trinity\",2" );
            out.println( "3,\"Trinity\",3" );
        }
        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships:" + join( new String[] { "ACTED_IN" }, ":" ), roles.getAbsolutePath());
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "id-spaces.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void badRelationshipsDefault() throws IOException
    {
        // GIVEN
        File movies = file( "ops", "movies9.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( "movieId:ID,title,year:int,:LABEL" );
            out.println( "tt0133093,\"The Matrix\",1999,Movie" );
            out.println( "tt0234215,\"The Matrix Reloaded\",2003,Movie;Sequel" );
            out.println( "tt0242653,\"The Matrix Revolutions\",2003,Movie;Sequel" );
        }

        File actors = file( "ops", "actors9.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID,name,:LABEL" );
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
        }

        File roles = file( "ops", "roles9.csv" );
        try (PrintStream out = new PrintStream( roles ))
        {
            out.println( ":START_ID,role,:END_ID,:TYPE" );
            out.println( "keanu,\"Neo\",tt0133093,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0234215,ACTED_IN" );
            out.println( "keanu,\"Neo\",tt0242653,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0133093,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0234215,ACTED_IN" );
            out.println( "laurence,\"Morpheus\",tt0242653,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0133093,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0234215,ACTED_IN" );
            out.println( "carrieanne,\"Trinity\",tt0242653,ACTED_IN" );
            out.println( "emil,\"Emil\",tt0133093,ACTED_IN" );
        }

        // WHEN
        File badFile = new File( directory.directory(), Configuration.BAD_FILE_NAME );
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships", roles.getAbsolutePath() );
        importTool( arguments );
        assertTrue( badFile.exists() );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printFileWithPathsRemoved( badFile, realDir, "bad-relationships-default-not-imported.bad.adoc" );
        printCommandToFile( arguments, realDir, "bad-relationships-default.adoc" );

        // THEN
        verifyData();
    }

    @Test
    public void badDuplicateNodesDefault() throws IOException
    {
        // GIVEN
        File actors = file( "ops", "actors10.csv" );
        try ( PrintStream out = new PrintStream( actors ) )
        {
            out.println( "personId:ID,name,:LABEL" );
            out.println( "keanu,\"Keanu Reeves\",Actor" );
            out.println( "laurence,\"Laurence Fishburne\",Actor" );
            out.println( "carrieanne,\"Carrie-Anne Moss\",Actor" );
            out.println( "laurence,\"Laurence Harvey\",Actor" );
        }

        // WHEN
        File badFile = new File( directory.directory(), Configuration.BAD_FILE_NAME );
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--skip-duplicate-nodes" );
        importTool( arguments );
        assertTrue( badFile.exists() );

        // DOCS
        String realDir = actors.getParentFile().getAbsolutePath();
        printFileWithPathsRemoved( badFile, realDir, "bad-duplicate-nodes-default-not-imported.bad.adoc" );
        printCommandToFile( arguments, realDir, "bad-duplicate-nodes-default.adoc" );

        // THEN
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx();
              ResourceIterator<Node> nodes = db.findNodes( DynamicLabel.label( "Actor" ) ) )
        {
            assertEquals( asSet( "Keanu Reeves", "Laurence Fishburne", "Carrie-Anne Moss" ), namesOf( nodes ) );
            tx.success();
        }
    }

    @Test
    public void propertyTypes() throws IOException
    {
        // GIVEN
        File movies = file( "ops", "movies7.csv" );
        try (PrintStream out = new PrintStream( movies ))
        {
            out.println( "movieId:ID,title,year:int,:LABEL" );
            out.println( "tt0099892,\"Joe Versus the Volcano\",1990,Movie" );
        }

        File actors = file( "ops", "actors7.csv" );
        try (PrintStream out = new PrintStream( actors ))
        {
            out.println( "personId:ID,name,:LABEL" );
            out.println( "meg,\"Meg Ryan\",Actor" );
        }

        File roles = file( "ops", "roles7.csv" );
        try (PrintStream out = new PrintStream( roles ))
        {
            out.println( ":START_ID,roles:string[],:END_ID,:TYPE" );
            out.println( "meg,\"DeDe;Angelica Graynamore;Patricia Graynamore\",tt0099892,ACTED_IN" );
        }

        // WHEN
        String[] arguments = arguments(
                "--into", directory.absolutePath(),
                "--nodes", movies.getAbsolutePath(),
                "--nodes", actors.getAbsolutePath(),
                "--relationships", roles.getAbsolutePath() );
        importTool( arguments );

        // DOCS
        String realDir = movies.getParentFile().getAbsolutePath();
        printCommandToFile( arguments, realDir, "property-types.adoc" );

        // THEN
        GraphDatabaseService db = new GraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = count( at( db ).getAllNodes() ), relationshipCount = 0;
            assertEquals( 2, nodeCount );

            for ( Relationship relationship : GlobalGraphOperations.at( db ).getAllRelationships() )
            {
                assertTrue( relationship.hasProperty( "roles" ) );

                String[] retrievedRoles = (String[]) relationship.getProperty( "roles" );
                assertEquals(3, retrievedRoles.length);

                relationshipCount++;
            }
            assertEquals( 1, relationshipCount );

            tx.success();
        }
        finally
        {
            db.shutdown();
        }
    }

    private Set<String> namesOf( Iterator<Node> nodes )
    {
        Set<String> names = new HashSet<>();
        while ( nodes.hasNext() )
        {
            names.add( (String) nodes.next().getProperty( "name" ) );
        }
        return names;
    }

    private void verifyData()
    {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( directory.absolutePath() );
        try ( Transaction tx = db.beginTx() )
        {
            int nodeCount = count( at( db ).getAllNodes() ), relationshipCount = 0, sequelCount = 0;
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
            if ( argument.contains( " " ) || argument.contains( "," )
                    || Arrays.asList( new String[] { ";", "|", "'" } ).contains( argument ) )
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
            for ( Options option : Options.values() )
            {
                out.print( option.manPageEntry() );
            }
        }
    }

    @Test
    public void printOptionsForManual() throws Exception
    {
        try ( PrintStream out = new PrintStream( file( "ops", "options.adoc" ) ) )
        {
            for ( Options option : Options.values() )
            {
                out.print( option.manualEntry() );
            }
        }
    }

    private void printFileWithPathsRemoved( File badFile, String realDir, String destinationFileName )
            throws IOException
    {
        String contents = readTextFile( badFile, Charsets.UTF_8 );
        String cleanedContents = contents.replace( realDir + File.separator, "" );
        writeToFile( file( "ops", destinationFileName ), cleanedContents, false );
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

    private void importTool( String[] arguments ) throws IOException
    {
        ImportTool.main( arguments, true );
    }
}
