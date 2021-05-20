/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.graphdb;

import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.helpers.collection.Iterators;
import org.neo4j.test.extension.ImpermanentDbmsExtension;

@ImpermanentDbmsExtension
public class RelationshipIndexingAcceptanceTest extends IndexingAcceptanceTestBase<RelationshipType,Relationship>
{
    @Override
    protected RelationshipType createToken( String name )
    {
        return RelationshipType.withName( name );
    }

    @Override
    protected List<Relationship> findEntitiesByTokenAndProperty( Transaction tx, RelationshipType type, String propertyName, Object value )
    {
        return Iterators.asList( tx.findRelationships( type, propertyName, value ) );
    }

    @Override
    protected Relationship createEntity( GraphDatabaseService db, Map<String,Object> properties, RelationshipType type )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node from = tx.createNode( Label.label( "test" ) );
            Node to = tx.createNode( Label.label( "test" ) );
            Relationship rel = from.createRelationshipTo( to, type );
            properties.forEach( rel::setProperty );
            tx.commit();
            return rel;
        }
    }

    @Override
    protected Relationship createEntity( Transaction tx, RelationshipType type )
    {
        Node from = tx.createNode( Label.label( "test" ) );
        Node to = tx.createNode( Label.label( "test" ) );
        return from.createRelationshipTo( to, type );
    }

    @Override
    protected void deleteEntity( Transaction tx, long id )
    {
        tx.getRelationshipById( id ).delete();
    }

    @Override
    protected Relationship getEntity( Transaction tx, long id )
    {
        return tx.getRelationshipById( id );
    }

    @Override
    protected IndexDefinition createIndex( GraphDatabaseService db, RelationshipType type, String... properties )
    {
        return SchemaAcceptanceTest.createIndex( db, type, properties );
    }

    @Override
    protected ResourceIterator<Relationship> findEntities( Transaction tx, RelationshipType type, String key, Object value )
    {
        return tx.findRelationships( type, key, value );
    }

    @Override
    protected Relationship findEntity( Transaction tx, RelationshipType type, String key, Object value )
    {
        return tx.findRelationship( type, key, value );
    }

    @Override
    protected String getMultipleEntitiesMessageTemplate()
    {
        return "Found multiple relationships with type: '%s', property name: 'name' " +
               "and property value: 'Stefan' while only one was expected.";
    }
}
