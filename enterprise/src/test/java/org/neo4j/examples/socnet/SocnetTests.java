package org.neo4j.examples.socnet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import java.util.Random;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;


public class SocnetTests {
    private static final Random r = new Random(System.currentTimeMillis());
    private GraphDatabaseService graphDb;
    private IndexService index;
    private PersonFactory personFactory;

    @Before
    public void setup() {
        graphDb = new EmbeddedGraphDatabase("target/socnetdb");
        index = new LuceneIndexService(graphDb);
        personFactory = new PersonFactory(graphDb, index);

        setupSocialNetwork(1000, 10);
    }

    @After
    public void teardown() {
        try {
            deleteSocialGraph(graphDb, personFactory);
        }
        finally {
            index.shutdown();
            graphDb.shutdown();
        }

    }

    @Test
    public void removingOneFriendIsHandledCleanly()
    {
        Person person1 = personFactory.getPersonByName("person#1");
        Person person2 = personFactory.getPersonByName("person#2");
        person1.addFriend(person2);

        int noOfFriends = IteratorUtil.count(person1.getFriends().iterator());

        person1.removeFriend(person2);

        int noOfFriendsAfterChange = IteratorUtil.count(person1.getFriends().iterator());

        assertThat(noOfFriends, equalTo(noOfFriendsAfterChange+1));
    }

    

    private void setupSocialNetwork(int nrOfPersons, int maxNrOfFriendsEach) {
        Transaction tx = graphDb.beginTx();
        try {
            for (int i = 0; i < 1000; i++) {
                personFactory.createPerson("person#" + i);
            }
            for (Person person : personFactory.getAllPersons()) {
                int nrOfFriends = r.nextInt(maxNrOfFriendsEach) + 1;
                for (int j = 0; j < nrOfFriends; j++) {
                    person.addFriend(personFactory.getPersonByName("person#" +
                            r.nextInt(nrOfPersons)));
                }
            }
            tx.success();
        }
        finally {
            tx.finish();
        }
    }

    private static void deleteSocialGraph(GraphDatabaseService graphDb,
                                          PersonFactory personFactory) {
        Transaction tx = graphDb.beginTx();
        try {
            for (Person person : personFactory.getAllPersons()) {
                personFactory.deletePerson(person);
            }
            tx.success();
        }
        finally {
            tx.finish();
        }
    }

}
