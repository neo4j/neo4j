package org.neo4j.examples.socnet;

import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class Main
{
    private static final Random r = new Random( System.currentTimeMillis() );

    public static void main( String args[] )
    {
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase(
                "target/socnetdb" );
        IndexService index = new LuceneIndexService( graphDb );
        try
        {
            PersonRepository personRepository = new PersonRepository( graphDb,
                    index );

            System.out.println( "Setup, creating social network..." );
            // create 1000 persons named "person#0", "person#1"...
            // each person will have random 1-10 (outgoing) friends
            setupSocialNetwork( graphDb, personRepository, 1000, 10 );

            // play around
            String name = "person#" + r.nextInt( 1000 );
            Person person1 = personRepository.getPersonByName( name );
            System.out.println( "\n" + person1 + " friends:" );
            for ( Person friend : person1.getFriends() )
            {
                System.out.println( "\t" + friend );
            }
            System.out.println( "\n" + person1
                                + " friends and friends of friends:" );
            for ( Person friend : person1.getFriendsOfFriends() )
            {
                System.out.println( "\t" + friend );
            }

            // get another person
            Person person2 = null;
            do
            {
                name = "person#" + r.nextInt( 1000 );
                person2 = personRepository.getPersonByName( name );
            }
            while ( person1.equals( person2 ) );

            System.out.println( "\n" + person1 + " is connected to " + person2
                                + " by path: " );
            int personCount = 0;
            for ( Person personInPath : person1.getPersonsFromMeTo( person2, 4 ) )
            {
                personCount++;
                System.out.println( "\t" + personInPath );
            }
            if ( personCount == 2 )
            {
                System.out.println( "\nRemoving friendship between " + person1
                                    + " and " + person2 );
                person1.removeFriend( person2 );
            }
            else
            {
                System.out.println( "\nCreating friendship between " + person1
                                    + " and " + person2 );
                person1.addFriend( person2 );
            }
            System.out.println( "\nAnd now " + person1 + " is connected to "
                                + person2 + " by path: " );
            for ( Person personInPath : person1.getPersonsFromMeTo( person2, 4 ) )
            {
                System.out.println( "\t" + personInPath );
            }

            System.out.print( "\ncleanup, deleting social network..." );
            deleteSocialGraph( graphDb, personRepository );
            System.out.println( " done" );
        }
        finally
        {
            index.shutdown();
            graphDb.shutdown();
        }
    }

    private static void setupSocialNetwork( GraphDatabaseService graphDb,
            PersonRepository personRepository, int nrOfPersons,
            int maxNrOfFriendsEach )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            for ( int i = 0; i < 1000; i++ )
            {
                personRepository.createPerson( "person#" + i );
            }
            for ( Person person : personRepository.getAllPersons() )
            {
                int nrOfFriends = r.nextInt( maxNrOfFriendsEach ) + 1;
                for ( int j = 0; j < nrOfFriends; j++ )
                {
                    person.addFriend( personRepository.getPersonByName( "person#"
                                                                        + r.nextInt( nrOfPersons ) ) );
                }
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private static void deleteSocialGraph( GraphDatabaseService graphDb,
            PersonRepository personRepository )
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            for ( Person person : personRepository.getAllPersons() )
            {
                personRepository.deletePerson( person );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
