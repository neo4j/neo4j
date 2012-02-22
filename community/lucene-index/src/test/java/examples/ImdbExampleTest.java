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
package examples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryParser.QueryParser.Operator;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.BatchInserterIndex;
import org.neo4j.graphdb.index.BatchInserterIndexProvider;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.index.RelationshipIndex;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.index.Neo4jTestCase;
import org.neo4j.index.impl.lucene.LuceneBatchInserterIndexProvider;
import org.neo4j.index.impl.lucene.LuceneIndex;
import org.neo4j.index.lucene.QueryContext;
import org.neo4j.index.lucene.ValueContext;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.impl.batchinsert.BatchInserter;
import org.neo4j.kernel.impl.batchinsert.BatchInserterImpl;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.TargetDirectory;

public class ImdbExampleTest
{
    private static EmbeddedGraphDatabase graphDb;
    private Transaction tx;

    @BeforeClass
    public static void setUpDb()
    {
        graphDb = new ImpermanentGraphDatabase();
        Transaction transaction = graphDb.beginTx();
        try
        {
            // START SNIPPET: createIndexes
            IndexManager index = graphDb.index();
            Index<Node> actors = index.forNodes( "actors" );
            Index<Node> movies = index.forNodes( "movies" );
            RelationshipIndex roles = index.forRelationships( "roles" );
            // END SNIPPET: createIndexes

            // START SNIPPET: createNodes
            // Actors
            Node reeves = graphDb.createNode();
            actors.add( reeves, "name", "Keanu Reeves" );
            Node bellucci = graphDb.createNode();
            actors.add( bellucci, "name", "Monica Bellucci" );
            // multiple values for a field
            actors.add( bellucci, "name", "La Bellucci" );
            // Movies
            Node theMatrix = graphDb.createNode();
            movies.add( theMatrix, "title", "The Matrix" );
            movies.add( theMatrix, "year", 1999 );
            Node theMatrixReloaded = graphDb.createNode();
            movies.add( theMatrixReloaded, "title", "The Matrix Reloaded" );
            movies.add( theMatrixReloaded, "year", 2003 );
            Node malena = graphDb.createNode();
            movies.add( malena, "title", "Malèna" );
            movies.add( malena, "year", 2000 );
            // END SNIPPET: createNodes

            reeves.setProperty( "name", "Keanu Reeves" );
            bellucci.setProperty( "name", "Monica Bellucci" );
            theMatrix.setProperty( "title", "The Matrix" );
            theMatrix.setProperty( "year", 1999 );
            theMatrixReloaded.setProperty( "title", "The Matrix Reloaded" );
            theMatrixReloaded.setProperty( "year", 2003 );
            malena.setProperty( "title", "Malèna" );
            malena.setProperty( "year", 2000 );

            // START SNIPPET: createRelationships
            // we need a relationship type
            DynamicRelationshipType ACTS_IN = DynamicRelationshipType.withName( "ACTS_IN" );
            // create relationships
            Relationship role1 = reeves.createRelationshipTo( theMatrix, ACTS_IN );
            roles.add( role1, "name", "Neo" );
            Relationship role2 = reeves.createRelationshipTo( theMatrixReloaded, ACTS_IN );
            roles.add( role2, "name", "Neo" );
            Relationship role3 = bellucci.createRelationshipTo( theMatrixReloaded, ACTS_IN );
            roles.add( role3, "name", "Persephone" );
            Relationship role4 = bellucci.createRelationshipTo( malena, ACTS_IN );
            roles.add( role4, "name", "Malèna Scordia" );
            // END SNIPPET: createRelationships

            role1.setProperty( "name", "Neo" );
            role2.setProperty( "name", "Neo" );
            role3.setProperty( "name", "Persephone" );
            role4.setProperty( "name", "Malèna Scordia" );

            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
    }

    @AfterClass
    public static void tearDownDb()
    {
        graphDb.shutdown();
    }

    @Before
    public void beginTx()
    {
        tx = graphDb.beginTx();
    }

    @After
    public void finishTx()
    {
        tx.finish();
    }

    private void rollbackTx()
    {
        finishTx();
        beginTx();
    }

    @Test
    public void checkIfIndexExists()
    {
        // START SNIPPET: checkIfExists
        IndexManager index = graphDb.index();
        boolean indexExists = index.existsForNodes( "actors" );
        // END SNIPPET: checkIfExists
        assertTrue( indexExists );
    }

    @Test
    public void deleteIndex()
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( TargetDirectory.forTest( getClass() ).directory( "delete", true ).getAbsolutePath() );
        Transaction transaction = graphDb.beginTx();
        try
        {
            // START SNIPPET: delete
            IndexManager index = graphDb.index();
            Index<Node> actors = index.forNodes( "actors" );
            actors.delete();
            // END SNIPPET: delete
            transaction.success();
        }
        finally
        {
            transaction.finish();
        }
        assertFalse( graphDb.index().existsForNodes( "actors" ) );
        graphDb.shutdown();
    }

    @Test
    public void removeFromIndex()
    {
        IndexManager index = graphDb.index();
        Index<Node> actors = index.forNodes( "actors" );
        Node bellucci = actors.get( "name", "Monica Bellucci" ).getSingle();
        assertNotNull( bellucci );

        // START SNIPPET: removeNodeFromIndex
        // completely remove bellucci from the actors index
        actors.remove( bellucci );
        // END SNIPPET: removeNodeFromIndex

        Node node = actors.get( "name", "Monica Bellucci" ).getSingle();
        assertEquals( null, node );
        node = actors.get( "name", "La Bellucci" ).getSingle();
        assertEquals( null, node );

        rollbackTx();

        // START SNIPPET: removeNodeFromIndex
        // remove any "name" entry of bellucci from the actors index
        actors.remove( bellucci, "name" );
        // END SNIPPET: removeNodeFromIndex

        node = actors.get( "name", "Monica Bellucci" ).getSingle();
        assertEquals( null, node );
        node = actors.get( "name", "La Bellucci" ).getSingle();
        assertEquals( null, node );

        rollbackTx();

        // START SNIPPET: removeNodeFromIndex
        // remove the "name" -> "La Bellucci" entry of bellucci
        actors.remove( bellucci, "name", "La Bellucci" );
        // END SNIPPET: removeNodeFromIndex

        node = actors.get( "name", "La Bellucci" ).getSingle();
        assertEquals( null, node );
        node = actors.get( "name", "Monica Bellucci" ).getSingle();
        assertEquals( bellucci, node );
    }

    @Test
    public void update()
    {
        IndexManager index = graphDb.index();
        Index<Node> actors = index.forNodes( "actors" );

        // START SNIPPET: update
        // create a node with a property
        Node fishburn = graphDb.createNode();
        fishburn.setProperty( "name", "Fishburn" );
        // index it
        actors.add( fishburn, "name", fishburn.getProperty( "name" ) );
        // END SNIPPET: update

        Node node = actors.get( "name", "Fishburn" ).getSingle();
        assertEquals( fishburn, node );

        // START SNIPPET: update
        // update the index entry
        actors.remove( fishburn, "name", fishburn.getProperty( "name" ) );
        fishburn.setProperty( "name", "Laurence Fishburn" );
        actors.add( fishburn, "name", fishburn.getProperty( "name" ) );
        // END SNIPPET: update

        node = actors.get( "name", "Fishburn" ).getSingle();
        assertEquals( null, node );
        node = actors.get( "name", "Laurence Fishburn" ).getSingle();
        assertEquals( fishburn, node );
    }

    @Test
    public void doGetForNodes()
    {
        Index<Node> actors = graphDb.index().forNodes( "actors" );

        // START SNIPPET: getSingleNode
        IndexHits<Node> hits = actors.get( "name", "Keanu Reeves" );
        Node reeves = hits.getSingle();
        // END SNIPPET: getSingleNode

        assertEquals( "Keanu Reeves", reeves.getProperty( "name" ) );
    }

    // @Test
    // public void getSameFromDifferentValuesO

    @Test
    public void doGetForRelationships()
    {
        RelationshipIndex roles = graphDb.index().forRelationships( "roles" );

        // START SNIPPET: getSingleRelationship
        Relationship persephone = roles.get( "name", "Persephone" ).getSingle();
        Node actor = persephone.getStartNode();
        Node movie = persephone.getEndNode();
        // END SNIPPET: getSingleRelationship

        assertEquals( "Monica Bellucci", actor.getProperty( "name" ) );
        assertEquals( "The Matrix Reloaded", movie.getProperty( "title" ) );

        @SuppressWarnings( "serial" ) List<String> expectedActors = new ArrayList<String>()
        {
            {
                add( "Keanu Reeves" );
                add( "Keanu Reeves" );
            }
        };
        List<String> foundActors = new ArrayList<String>();

        // START SNIPPET: getRelationships
        for ( Relationship role : roles.get( "name", "Neo" ) )
        {
            // this will give us Reeves twice
            Node reeves = role.getStartNode();
            // END SNIPPET: getRelationships
            foundActors.add( (String) reeves.getProperty( "name" ) );
            // START SNIPPET: getRelationships
        }
        // END SNIPPET: getRelationships

        assertEquals( expectedActors, foundActors );
    }

    @Test
    public void doQueriesForNodes()
    {
        IndexManager index = graphDb.index();
        Index<Node> actors = index.forNodes( "actors" );
        Index<Node> movies = index.forNodes( "movies" );
        Set<String> found = new HashSet<String>();
        @SuppressWarnings( "serial" ) Set<String> expectedActors = new HashSet<String>()
        {
            {
                add( "Monica Bellucci" );
                add( "Keanu Reeves" );
            }
        };
        @SuppressWarnings( "serial" ) Set<String> expectedMovies = new HashSet<String>()
        {
            {
                add( "The Matrix" );
            }
        };

        // START SNIPPET: actorsQuery
        for ( Node actor : actors.query( "name", "*e*" ) )
        {
            // This will return Reeves and Bellucci
            // END SNIPPET: actorsQuery
            found.add( (String) actor.getProperty( "name" ) );
            // START SNIPPET: actorsQuery
        }
        // END SNIPPET: actorsQuery
        assertEquals( expectedActors, found );
        found.clear();

        // START SNIPPET: matrixQuery
        for ( Node movie : movies.query( "title:*Matrix* AND year:1999" ) )
        {
            // This will return "The Matrix" from 1999 only.
            // END SNIPPET: matrixQuery
            found.add( (String) movie.getProperty( "title" ) );
            // START SNIPPET: matrixQuery
        }
        // END SNIPPET: matrixQuery
        assertEquals( expectedMovies, found );

        // START SNIPPET: matrixSingleQuery
        Node matrix = movies.query( "title:*Matrix* AND year:2003" ).getSingle();
        // END SNIPPET: matrixSingleQuery
        assertEquals( "The Matrix Reloaded", matrix.getProperty( "title" ) );

        // START SNIPPET: queryWithScore
        IndexHits<Node> hits = movies.query( "title", "The*" );
        for ( Node movie : hits )
        {
            System.out.println( movie.getProperty( "title" ) + " " + hits.currentScore() );
            // END SNIPPET: queryWithScore
            assertTrue( ( (String) movie.getProperty( "title" ) ).startsWith( "The" ) );
            // START SNIPPET: queryWithScore
        }
        // END SNIPPET: queryWithScore
        assertEquals( 2, hits.size() );

        // START SNIPPET: queryWithRelevance
        hits = movies.query( "title", new QueryContext( "The*" ).sortByScore() );
        // END SNIPPET: queryWithRelevance
        float previous = Float.MAX_VALUE;
        // START SNIPPET: queryWithRelevance
        for ( Node movie : hits )
        {
            // hits sorted by relevance (score)
            // END SNIPPET: queryWithRelevance
            assertTrue( hits.currentScore() <= previous );
            previous = hits.currentScore();
            // START SNIPPET: queryWithRelevance
        }
        // END SNIPPET: queryWithRelevance
        assertEquals( 2, hits.size() );

        // START SNIPPET: termQuery
        // a TermQuery will give exact matches
        Node actor = actors.query( new TermQuery( new Term( "name", "Keanu Reeves" ) ) ).getSingle();
        // END SNIPPET: termQuery
        assertEquals( "Keanu Reeves", actor.getProperty( "name" ) );

        Node theMatrix = movies.get( "title", "The Matrix" ).getSingle();
        Node theMatrixReloaded = movies.get( "title", "The Matrix Reloaded" ).getSingle();
        Node malena = movies.get( "title", "Malèna" ).getSingle();

        // START SNIPPET: wildcardTermQuery
        hits = movies.query( new WildcardQuery( new Term( "title", "The Matrix*" ) ) );
        for ( Node movie : hits )
        {
            System.out.println( movie.getProperty( "title" ) );
            // END SNIPPET: wildcardTermQuery
            assertTrue( ( (String) movie.getProperty( "title" ) ).startsWith( "The Matrix" ) );
            // START SNIPPET: wildcardTermQuery
        }
        // END SNIPPET: wildcardTermQuery
        assertEquals( 2, hits.size() );

        // START SNIPPET: numericRange
        movies.add( theMatrix, "year-numeric", new ValueContext( 1999 ).indexNumeric() );
        movies.add( theMatrixReloaded, "year-numeric", new ValueContext( 2003 ).indexNumeric() );
        movies.add( malena, "year-numeric",  new ValueContext( 2000 ).indexNumeric() );

        int from = 1997;
        int to = 1999;
        hits = movies.query( QueryContext.numericRange( "year-numeric", from, to ) );
        // END SNIPPET: numericRange
        assertEquals( theMatrix, hits.getSingle() );

        // START SNIPPET: sortedNumericRange
        hits = movies.query(
                QueryContext.numericRange( "year-numeric", from, null )
                  .sortNumeric( "year-numeric", false ) );
        // END SNIPPET: sortedNumericRange
        List<String> sortedMovies = new ArrayList<String>();
        @SuppressWarnings( "serial" ) List<String> expectedSortedMovies = new ArrayList<String>()
        {
            {
                add( "The Matrix" );
                add( "Malèna" );
                add( "The Matrix Reloaded" );
            }
        };
        for ( Node hit : hits )
        {
            sortedMovies.add( (String) hit.getProperty( "title" ) );
        }
        assertEquals( expectedSortedMovies, sortedMovies );
        
        // START SNIPPET: exclusiveRange
        movies.add( theMatrix, "score", new ValueContext( 8.7 ).indexNumeric() );
        movies.add( theMatrixReloaded, "score", new ValueContext( 7.1 ).indexNumeric() );
        movies.add( malena, "score", new ValueContext( 7.4 ).indexNumeric() );
        
        // include 8.0, exclude 9.0
        hits = movies.query( QueryContext.numericRange( "score", 8.0, 9.0, true, false ) );
        // END SNIPPET: exclusiveRange
        found.clear();
        for ( Node hit : hits )
        {
            found.add( (String) hit.getProperty( "title" ) );
        }
        assertEquals( expectedMovies, found );        

        // START SNIPPET: compoundQueries
        hits = movies.query( "title:*Matrix* AND year:1999" );
        // END SNIPPET: compoundQueries

        assertEquals( theMatrix, hits.getSingle() );

        // START SNIPPET: defaultOperator
        QueryContext query = new QueryContext( "title:*Matrix* year:1999" ).defaultOperator( Operator.AND );
        hits = movies.query( query );
        // END SNIPPET: defaultOperator
        // with OR the result would be 2 hits
        assertEquals( 1, hits.size() );

        // START SNIPPET: sortedResult
        hits = movies.query( "title", new QueryContext( "*" ).sort( "title" ) );
        for ( Node hit : hits )
        {
            // all movies with a title in the index, ordered by title
        }
        // END SNIPPET: sortedResult
        assertEquals( 3, hits.size() );
        // START SNIPPET: sortedResult
        // or
        hits = movies.query( new QueryContext( "title:*" ).sort( "year", "title" ) );
        for ( Node hit : hits )
        {
            // all movies with a title in the index, ordered by year, then title
        }
        // END SNIPPET: sortedResult
        assertEquals( 3, hits.size() );
    }

    @Test
    public void doQueriesForRelationships()
    {
        IndexManager index = graphDb.index();
        RelationshipIndex roles = index.forRelationships( "roles" );
        Index<Node> actors = graphDb.index().forNodes( "actors" );
        Index<Node> movies = index.forNodes( "movies" );

        Node reeves = actors.get( "name", "Keanu Reeves" ).getSingle();
        Node theMatrix = movies.get( "title", "The Matrix" ).getSingle();

        // START SNIPPET: queryForRelationships
        // find relationships filtering on start node
        // using exact matches
        IndexHits<Relationship> reevesAsNeoHits;
        reevesAsNeoHits = roles.get( "name", "Neo", reeves, null );
        Relationship reevesAsNeo = reevesAsNeoHits.iterator().next();
        reevesAsNeoHits.close();
        // END SNIPPET: queryForRelationships
        assertEquals( "Neo", reevesAsNeo.getProperty( "name" ) );
        Node actor = reevesAsNeo.getStartNode();
        assertEquals( reeves, actor );

        // START SNIPPET: queryForRelationships
        // find relationships filtering on end node
        // using a query
        IndexHits<Relationship> matrixNeoHits;
        matrixNeoHits = roles.query( "name", "*eo", null, theMatrix );
        Relationship matrixNeo = matrixNeoHits.iterator().next();
        matrixNeoHits.close();
        // END SNIPPET: queryForRelationships
        assertEquals( "Neo", matrixNeo.getProperty( "name" ) );
        actor = matrixNeo.getStartNode();
        assertEquals( reeves, actor );

        // START SNIPPET: queryForRelationshipType
        // find relationships filtering on end node
        // using a relationship type.
        // this is how to add it to the index:
        roles.add( reevesAsNeo, "type", reevesAsNeo.getType().name() );
        // Note that to use a compound query, we can't combine committed
        // and uncommitted index entries, so we'll commit before querying:
        tx.success();
        tx.finish();
        // and now we can search for it:
        IndexHits<Relationship> typeHits;
        typeHits = roles.query( "type:ACTS_IN AND name:Neo", null, theMatrix );
        Relationship typeNeo = typeHits.iterator().next();
        typeHits.close();
        // END SNIPPET: queryForRelationshipType
        assertEquals( "Neo", typeNeo.getProperty( "name" ) );
        actor = matrixNeo.getStartNode();
        assertEquals( reeves, actor );
    }

    @Test
    public void fulltext()
    {
        // START SNIPPET: fulltext
        IndexManager index = graphDb.index();
        Index<Node> fulltextMovies = index.forNodes( "movies-fulltext",
                MapUtil.stringMap( IndexManager.PROVIDER, "lucene", "type", "fulltext" ) );
        // END SNIPPET: fulltext

        Index<Node> movies = index.forNodes( "movies" );
        Node theMatrix = movies.get( "title", "The Matrix" ).getSingle();
        Node theMatrixReloaded = movies.get( "title", "The Matrix Reloaded" ).getSingle();

        // START SNIPPET: fulltext
        fulltextMovies.add( theMatrix, "title", "The Matrix" );
        fulltextMovies.add( theMatrixReloaded, "title", "The Matrix Reloaded" );
        // search in the fulltext index
        Node found = fulltextMovies.query( "title", "reloAdEd" ).getSingle();
        // END SNIPPET: fulltext
        assertEquals( theMatrixReloaded, found );
    }

    @Test
    public void cacheSettings()
    {
        // START SNIPPET: cache
        Index<Node> index = graphDb.index().forNodes( "actors" );
        ( (LuceneIndex<Node>) index ).setCacheCapacity( "name", 300000 );
        // END SNIPPET: cache
    }

    @Test
    public void batchInsert()
    {
        Neo4jTestCase.deleteFileOrDirectory( new File(
                "target/neo4jdb-batchinsert" ) );
        // START SNIPPET: batchInsert
        BatchInserter inserter = new BatchInserterImpl( "target/neo4jdb-batchinsert" );
        BatchInserterIndexProvider indexProvider = new LuceneBatchInserterIndexProvider( inserter );
        BatchInserterIndex actors = indexProvider.nodeIndex( "actors", MapUtil.stringMap( "type", "exact" ) );
        actors.setCacheCapacity( "name", 100000 );

        Map<String, Object> properties = MapUtil.map( "name", "Keanu Reeves" );
        long node = inserter.createNode( properties );
        actors.add( node, properties );
        
        //make the changes visible for reading, use this sparsely, requires IO!
        actors.flush();
        
        // Make sure to shut down the index provider
        indexProvider.shutdown();
        inserter.shutdown();
        // END SNIPPET: batchInsert

        GraphDatabaseService db = new EmbeddedGraphDatabase(
                "target/neo4jdb-batchinsert" );
        Index<Node> index = db.index()
                .forNodes( "actors" );
        Node reeves = index.get( "name", "Keanu Reeves" )
                .next();
        assertEquals( node, reeves.getId() );
        db.shutdown();
    }
}
