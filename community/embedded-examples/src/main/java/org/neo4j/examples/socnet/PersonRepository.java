/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.socnet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.helpers.collection.IterableWrapper;

import static org.neo4j.examples.socnet.RelTypes.A_PERSON;

public class PersonRepository
{
    private final GraphDatabaseService graphDb;
    private final Index<Node> index;
    private final Node personRefNode;

    public PersonRepository( GraphDatabaseService graphDb, Index<Node> index )
    {
        this.graphDb = graphDb;
        this.index = index;

        personRefNode = getPersonsRootNode( graphDb );
    }

    private Node getPersonsRootNode( GraphDatabaseService graphDb )
    {
        Index<Node> referenceIndex = graphDb.index().forNodes( "reference");
        IndexHits<Node> result = referenceIndex.get( "reference", "person" );
        if (result.hasNext())
        {
            return result.next();
        }

        Node refNode = this.graphDb.createNode();
        refNode.setProperty( "reference", "persons" );
        referenceIndex.add( refNode, "reference", "persons" );
        return refNode;
    }

    public Person createPerson( String name ) throws Exception
    {
        // to guard against duplications we use the lock grabbed on ref node
        // when
        // creating a relationship and are optimistic about person not existing
        Node newPersonNode = graphDb.createNode();
        personRefNode.createRelationshipTo( newPersonNode, A_PERSON );
        // lock now taken, we can check if  already exist in index
        Node alreadyExist = index.get( Person.NAME, name ).getSingle();
        if ( alreadyExist != null )
        {
            throw new Exception( "Person with this name already exists " );
        }
        newPersonNode.setProperty( Person.NAME, name );
        index.add( newPersonNode, Person.NAME, name );
        return new Person( newPersonNode );
    }

    public Person getPersonByName( String name )
    {
        Node personNode = index.get( Person.NAME, name ).getSingle();
        if ( personNode == null )
        {
            throw new IllegalArgumentException( "Person[" + name
                    + "] not found" );
        }
        return new Person( personNode );
    }

    public void deletePerson( Person person )
    {
        Node personNode = person.getUnderlyingNode();
        index.remove( personNode, Person.NAME, person.getName() );
        for ( Person friend : person.getFriends() )
        {
            person.removeFriend( friend );
        }
        personNode.getSingleRelationship( A_PERSON, Direction.INCOMING ).delete();

        for ( StatusUpdate status : person.getStatus() )
        {
            Node statusNode = status.getUnderlyingNode();
            for ( Relationship r : statusNode.getRelationships() )
            {
                r.delete();
            }
            statusNode.delete();
        }

        personNode.delete();
    }

    public Iterable<Person> getAllPersons()
    {
        return new IterableWrapper<Person, Relationship>(
                personRefNode.getRelationships( A_PERSON ) )
        {
            @Override
            protected Person underlyingObjectToObject( Relationship personRel )
            {
                return new Person( personRel.getEndNode() );
            }
        };
    }
}
