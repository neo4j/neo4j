/**
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
package org.neo4j.examples;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.cypher.javacompat.ExecutionEngine;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import scala.actors.threadpool.Arrays;

public class CypherSql
{
    private static final int COLUMN_MAX_WIDTH = 25;
    private static final String LINE_SEGMENT = new String(
            new char[COLUMN_MAX_WIDTH] ).replace( '\0', '-' );
    private static final String SPACE_SEGMENT = new String(
            new char[COLUMN_MAX_WIDTH] ).replace( '\0', ' ' );

    private Connection sqldb;
    private String identifierQuoteString;
    private final GraphDatabaseService graphdb;
    private final ExecutionEngine engine;
    List<TestData> queries;

    public CypherSql( final GraphDatabaseService graphdb )
    {
        this.graphdb = graphdb;
        this.engine = new ExecutionEngine( graphdb );
    }

    public static void main( String[] args ) throws SQLException
    {
        CypherSql instance = new CypherSql( new EmbeddedGraphDatabase(
                "target/cyphersql" + System.currentTimeMillis() ) );
        instance.createDbs();
        instance.run();
        instance.shutdown();
    }

    void createDbs() throws SQLException
    {
        sqldb = DriverManager.getConnection( "jdbc:hsqldb:mem:cyphersqltests;shutdown=true" );
        sqldb.setAutoCommit( true );
        identifierQuoteString = sqldb.getMetaData()
                .getIdentifierQuoteString();

        createPersons();
        createGroups();
        createEmail();

        /*

        CREATE TABLE "Person"(name VARCHAR(20), id INT PRIMARY KEY, age INT, hair VARCHAR(20));
        CREATE TABLE "Group"(name VARCHAR(20), id INT PRIMARY KEY, belongs_to_group_id INT);
        CREATE TABLE "Person_Group"(person_id INT, group_id INT);
        CREATE TABLE "Email"(address VARCHAR(20), comment VARCHAR(20), person_id INT);


        INSERT INTO "Person" (name, id, age, hair) VALUES ('Anakin', 1, 20, 'blonde');
        INSERT INTO "Person" (name, id, age, hair) VALUES ('Bridget', 2, 40, 'blonde');

        INSERT INTO "Group" (name, id) VALUES ('User', 1);
        INSERT INTO "Group" (name, id) VALUES ('Manager', 2);
        INSERT INTO "Group" (name, id) VALUES ('Technichian', 3);
        INSERT INTO "Group" (name, id) VALUES ('Admin', 4);

        INSERT INTO "Person_Group" (person_id,group_id) VALUES ((SELECT "Person".id FROM "Person" WHERE "Person".name = 'Bridget' ), (SELECT "Group".id FROM "Group" WHERE "Group".name = 'Admin' ));
        INSERT INTO "Person_Group" (person_id,group_id) VALUES ((SELECT "Person".id FROM "Person" WHERE "Person".name = 'Anakin' ), (SELECT "Group".id FROM "Group" WHERE "Group".name = 'User' ));

        UPDATE "Group" SET belongs_to_group_id = (SELECT "Group".id FROM "Group" WHERE "Group".name ='Technichian' ) WHERE name='Admin';
        UPDATE "Group" SET belongs_to_group_id = (SELECT "Group".id FROM "Group" WHERE "Group".name ='User' ) WHERE name='Technichian';
        UPDATE "Group" SET belongs_to_group_id = (SELECT "Group".id FROM "Group" WHERE "Group".name ='User' ) WHERE name='Manager';

        INSERT INTO "Email" (address, comment) VALUES ('anakin@example.com', 'home');
        INSERT INTO "Email" (address, comment) VALUES ('anakin@example.org', 'work');


        UPDATE "Email" SET person_id = (SELECT "Person".id FROM "Person" WHERE "Person".name ='Anakin' ) WHERE address='anakin@example.com';
        UPDATE "Email" SET person_id = (SELECT "Person".id FROM "Person" WHERE "Person".name ='Anakin' ) WHERE address='anakin@example.org';


        WITH RECURSIVE TransitiveGroup(id, name, belongs_to_group_id) AS (
        SELECT child.id, child.name, child.belongs_to_group_id
        FROM "Group" child
        WHERE child.name='Admin'
        UNION ALL
        SELECT parent.id, parent.name, parent.belongs_to_group_id
        FROM TransitiveGroup child, "Group" parent
        WHERE parent.id = child.belongs_to_group_id
        ) SELECT * FROM TransitiveGroup

         */

        queries = new ArrayList<TestData>()
        {
            {
                add( new TestData(
                        "Start",
                        "SELECT * FROM `Person` WHERE name = 'Anakin'".replace(
                                "`", identifierQuoteString ),
                        "START person=node:Person(name = 'Anakin') RETURN person",
                        "Anakin", "20", "1 row" ) );
                add( new TestData(
                        "Match",
                        "SELECT `Email`.* FROM `Person` JOIN `Email` ON `Person`.id = `Email`.person_id WHERE `Person`.name = 'Anakin'".replace(
                                "`", identifierQuoteString ),
                        "START person=node:Person(name = 'Anakin') MATCH person-[:email]->email RETURN email",
                        "anakin@example.com", "anakin@example.org", "2 rows" ) );
                add( new TestData(
                        "JoinEntity",
                        "SELECT `Group`.*, `Person_Group`.* FROM `Person` JOIN `Person_Group` ON `Person`.id = `Person_Group`.person_id JOIN `Group` ON `Person_Group`.Group_id=`Group`.id WHERE `Person`.name = 'Bridget'".replace(
                                "`", identifierQuoteString ),
                        "START person=node:Person(name = 'Bridget') MATCH person-[r:belongs_to]->group RETURN group, r",
                        "Admin", "1 row" ) );
                add( new TestData(
                        "LeftJoin",
                        "SELECT `Person`.name, `Email`.address FROM `Person` LEFT JOIN `Email` ON `Person`.id = `Email`.person_id".replace(
                                "`", identifierQuoteString ),
                        "START person=node:Person('name: *') MATCH person-[?:email]->email RETURN person.name, email.address?",
                        "Anakin", "anakin@example.org", "Bridget", "<null>",
                        "3 rows" ) );
                add( new TestData(
                        "RecursiveJoin",
                        /*(*/ null /*"WITH RECURSIVE TransitiveGroup(id, name, belongs_to_group_id) AS ( "
                          + "SELECT child.id, child.name, child.belongs_to_group_id "
                          + "FROM `Group` child "
                          + "WHERE child.name='Admin' "
                          + "UNION ALL SELECT parent.id, parent.name, parent.belongs_to_group_id "
                          + "FROM TransitiveGroup child, `Group` parent "
                          + "WHERE parent.id = child.belongs_to_group_id "
                          + ") SELECT * FROM TransitiveGroup" ).replace( "`",
                                identifierQuoteString )*/,
                        "START person=node:Person('name: Bridget') "
                                + "MATCH person-[:belongs_to*]->group RETURN person.name, group.name",
                        "Bridget", "Admin", "Technichian", "User", "3 rows" ) );
                add( new TestData(
                        "Where",
                        "SELECT * FROM `Person` WHERE `Person`.age > 35 AND `Person`.hair = 'blonde'".replace(
                                "`", identifierQuoteString ),
                        "START person=node:Person('name: *') WHERE person.age > 35 AND person.hair = 'blonde' RETURN person",
                        "Bridget", "blonde", "1 row" ) );
                add( new TestData(
                        "Return",
                        "SELECT `Person`.name, count(*) FROM `Person` GROUP BY `Person`.name ORDER BY `Person`.name".replace(
                                "`", identifierQuoteString ),
                        "START person=node:Person('name: *') RETURN person.name, count(*) ORDER BY person.name",
                        "Bridget", "Anakin", "2 rows" ) );
            }
        };
    }

    private void createPersons() throws SQLException
    {
        String tableName = "Person";
        String tableDefinition = "name VARCHAR(20), id INT PRIMARY KEY, age INT, hair VARCHAR(20)";
        String[] fields = new String[] { "name", "id", "age", "hair" };
        Object[][] values = new Object[][] { { "Anakin", 1, 20, "blonde" },
                { "Bridget", 2, 40, "blonde" } };

        createEntities( tableName, tableDefinition, fields, values );
    }

    private void createGroups() throws SQLException
    {
        String tableName = "Group";
        String tableDefinition = "name VARCHAR(20), id INT PRIMARY KEY, belongs_to_group_id INT";
        String[] fields = new String[] { "name", "id" };
        Object[][] values = new Object[][] { { "User", 1 }, { "Manager", 2 },
                { "Technichian", 3 }, { "Admin", 4 } };

        createEntities( tableName, tableDefinition, fields, values );

        String sourceEntity = "Person";
        String sourceMatchAttribute = "name";
        String targetEntity = tableName;
        String targetMatchAttribute = "name";

        Object[][] relationships = new Object[][] { { "Bridget", "Admin" },
                { "Anakin", "User" } };

        createRelationships( sourceEntity, sourceMatchAttribute, targetEntity,
                targetMatchAttribute, relationships, "belongs_to" );

        sourceEntity = tableName;

        relationships = new Object[][] { { "Technichian", "Admin" },
                { "User", "Technichian" }, { "User", "Manager" } };

        createSelfRelationships( sourceEntity, sourceMatchAttribute,
                relationships, "belongs_to" );
    }

    private void createEmail() throws SQLException
    {
        String tableName = "Email";
        String tableDefinition = "address VARCHAR(20), comment VARCHAR(20), person_id INT";
        String[] fields = new String[] { "address", "comment" };
        Object[][] values = new Object[][] { { "anakin@example.com", "home" },
                { "anakin@example.org", "work" } };

        createEntities( tableName, tableDefinition, fields, values );

        String sourceEntity = "Person";
        String sourceMatchAttribute = "name";
        String targetEntity = tableName;
        String targetMatchAttribute = fields[0];
        Object[][] relationships = new Object[][] {
                { "Anakin", "anakin@example.com" },
                { "Anakin", "anakin@example.org" } };
        createSimpleRelationships( sourceEntity, sourceMatchAttribute,
                targetEntity, targetMatchAttribute, relationships, "email" );
    }

    private void createSimpleRelationships( String sourceEntity,
            String sourceMatchAttribute, String targetEntity,
            String targetMatchAttribute, Object[][] relationships,
            String relationshipType ) throws SQLException
    {
        String targetAttribute = sourceEntity.toLowerCase() + "_id";
        PreparedStatement prep = prepareSimpleRelationshipStatement(
                sourceEntity, sourceMatchAttribute, targetEntity,
                targetMatchAttribute, targetAttribute );
        insertIntoRdbms( prep, relationships );
        createRelationshipsInGraphdb( sourceEntity, sourceMatchAttribute,
                targetEntity, targetMatchAttribute, relationships,
                relationshipType );
    }

    private void createSelfRelationships( String entity, String matchAttribute,
            Object[][] relationships, String relationshipType )
            throws SQLException
    {
        String targetAttribute = relationshipType + "_" + entity.toLowerCase()
                                 + "_id";
        PreparedStatement prep = prepareSelfJoinRelationshipStatement( entity,
                matchAttribute, targetAttribute );
        insertIntoRdbms( prep, relationships );
        // get the correct direction for graphdb
        for ( Object[] objects : relationships )
        {
            Collections.reverse( Arrays.asList( objects ) );
        }
        createRelationshipsInGraphdb( entity, matchAttribute, entity,
                matchAttribute, relationships, relationshipType );
    }

    private void createRelationships( String sourceEntity,
            String sourceMatchAttribute, String targetEntity,
            String targetMatchAttribute, Object[][] relationships,
            String relationshipType ) throws SQLException
    {
        String relationship = sourceEntity + "_" + targetEntity;
        PreparedStatement prep = prepareRelationshipStatement( sourceEntity,
                sourceMatchAttribute, targetEntity, targetMatchAttribute,
                relationship );
        insertIntoRdbms( prep, relationships );
        createRelationshipsInGraphdb( sourceEntity, sourceMatchAttribute,
                targetEntity, targetMatchAttribute, relationships,
                relationshipType );
    }

    private void createRelationshipsInGraphdb( String sourceEntity,
            String sourceMatchAttribute, String targetEntity,
            String targetMatchAttribute, Object[][] relationships,
            String relationshipType )
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            RelationshipType type = DynamicRelationshipType.withName( relationshipType );
            Index<Node> sourceIndex = graphdb.index()
                    .forNodes( sourceEntity );
            Index<Node> targetIndex = graphdb.index()
                    .forNodes( targetEntity );
            for ( Object[] relationship : relationships )
            {
                Node sourceNode = sourceIndex.get( sourceMatchAttribute,
                        relationship[0] )
                        .getSingle();
                Node targetNode = targetIndex.get( targetMatchAttribute,
                        relationship[1] )
                        .getSingle();
                sourceNode.createRelationshipTo( targetNode, type );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void createEntities( String tableName, String tableDefinition,
            String[] fields, Object[][] values ) throws SQLException
    {
        createTable( tableName, tableDefinition );
        PreparedStatement prep = prepareInsertStatement( tableName, fields );
        insertIntoRdbms( prep, values );
        insertIntoGraphdb( tableName, fields, values );
    }

    private void insertIntoGraphdb( String tableName, String[] fields,
            Object[][] values ) throws SQLException
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Index<Node> index = graphdb.index()
                    .forNodes( tableName );
            for ( Object[] value : values )
            {
                Node node = graphdb.createNode();
                for ( int i = 0; i < fields.length; i++ )
                {
                    node.setProperty( fields[i], value[i] );
                    if ( i == 0 )
                    {
                        index.add( node, fields[i], value[i] );
                    }
                }
            }

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private void insertIntoRdbms( PreparedStatement prep, Object[][] values )
            throws SQLException
    {
        for ( Object[] value : values )
        {
            ParameterMetaData metaData = prep.getParameterMetaData();
            for ( int i = 0; i < metaData.getParameterCount(); i++ )
            {
                int parameterType = metaData.getParameterType( i + 1 );
                switch ( parameterType )
                {
                case Types.INTEGER:
                    prep.setInt( i + 1, (Integer) value[i] );
                    break;
                case Types.VARCHAR:
                    prep.setString( i + 1, (String) value[i] );
                    break;
                default:
                    throw new RuntimeException( "Unknown SQL type: "
                                                + parameterType );
                }
            }
            prep.executeUpdate();
        }
        prep.close();
    }

    private void createTable( String name, String definition )
            throws SQLException
    {
        String sql = "CREATE TABLE " + identifierQuoteString + name
                     + identifierQuoteString + "(" + definition + ")";
        System.out.println( sql );
        Statement statement = sqldb.createStatement();
        statement.execute( sql );
    }

    private PreparedStatement prepareInsertStatement( String tableName,
            String[] fields ) throws SQLException
    {
        StringBuilder sql = new StringBuilder( 100 );
        String fieldList = String.valueOf( Arrays.asList( fields ) );
        fieldList = fieldList.substring( 1, fieldList.length() - 1 );
        String valueList = new String( new char[fields.length - 1] ).replace(
                "\0", ", ?" );
        sql.append( "INSERT INTO " )
                .append( identifierQuoteString )
                .append( tableName )
                .append( identifierQuoteString )
                .append( " (" )
                .append( fieldList )
                .append( ") VALUES (?" )
                .append( valueList )
                .append( ")" );
        return sqldb.prepareStatement( sql.toString() );
    }

    private PreparedStatement prepareSimpleRelationshipStatement(
            String sourceEntity, String sourceMatchAttribute,
            String targetEntity, String targetMatchAttribute,
            String targetAttribute ) throws SQLException
    {
        StringBuilder sql = new StringBuilder( 100 );
        sql.append( "UPDATE " )
                .append( identifierQuoteString )
                .append( targetEntity )
                .append( identifierQuoteString )
                .append( " SET " )
                .append( sourceEntity.toLowerCase() )
                .append( "_id = (SELECT " )
                .append( identifierQuoteString )
                .append( sourceEntity )
                .append( identifierQuoteString )
                .append( ".id FROM " )
                .append( identifierQuoteString )
                .append( sourceEntity )
                .append( identifierQuoteString )
                .append( " WHERE " )
                .append( identifierQuoteString )
                .append( sourceEntity )
                .append( identifierQuoteString )
                .append( "." )
                .append( sourceMatchAttribute )
                .append( " =? ) WHERE " )
                .append( targetMatchAttribute )
                .append( "=?" );
        return sqldb.prepareStatement( sql.toString() );
    }

    private PreparedStatement prepareSelfJoinRelationshipStatement(
            String entity, String matchAttribute, String targetAttribute )
            throws SQLException
    {
        StringBuilder sql = new StringBuilder( 100 );
        sql.append( "UPDATE " )
                .append( identifierQuoteString )
                .append( entity )
                .append( identifierQuoteString )
                .append( " SET " )
                .append( targetAttribute )
                .append( " = (SELECT " )
                .append( identifierQuoteString )
                .append( entity )
                .append( identifierQuoteString )
                .append( ".id FROM " )
                .append( identifierQuoteString )
                .append( entity )
                .append( identifierQuoteString )
                .append( " WHERE " )
                .append( identifierQuoteString )
                .append( entity )
                .append( identifierQuoteString )
                .append( "." )
                .append( matchAttribute )
                .append( " =? ) WHERE " )
                .append( matchAttribute )
                .append( "=?" );
        return sqldb.prepareStatement( sql.toString() );
    }

    private PreparedStatement prepareRelationshipStatement(
            String sourceEntity, String sourceMatchAttribute,
            String targetEntity, String targetMatchAttribute, String joinEntity )
            throws SQLException
    {
        String sourceColumnName = sourceEntity.toLowerCase() + "_id";
        String targetColumnName = targetEntity.toLowerCase() + "_id";
        StringBuilder sql = new StringBuilder( 100 );
        sql.append( sourceColumnName )
                .append( " INT, " )
                .append( targetColumnName )
                .append( " INT" );
        createTable( joinEntity, sql.toString() );
        sql.setLength( 0 );
        sql.append( "INSERT INTO " )
                .append( identifierQuoteString )
                .append( joinEntity )
                .append( identifierQuoteString )
                .append( " (" )
                .append( sourceColumnName )
                .append( "," )
                .append( targetColumnName )
                .append( ") VALUES ((SELECT " )
                .append( identifierQuoteString )
                .append( sourceEntity )
                .append( identifierQuoteString )
                .append( ".id FROM " )
                .append( identifierQuoteString )
                .append( sourceEntity )
                .append( identifierQuoteString )
                .append( " WHERE " )
                .append( identifierQuoteString )
                .append( sourceEntity )
                .append( identifierQuoteString )
                .append( "." )
                .append( sourceMatchAttribute )
                .append( " = ? ), (SELECT " )
                .append( identifierQuoteString )
                .append( targetEntity )
                .append( identifierQuoteString )
                .append( ".id FROM " )
                .append( identifierQuoteString )
                .append( targetEntity )
                .append( identifierQuoteString )
                .append( " WHERE " )
                .append( identifierQuoteString )
                .append( targetEntity )
                .append( identifierQuoteString )
                .append( "." )
                .append( targetMatchAttribute )
                .append( " = ? ))" );
        return sqldb.prepareStatement( sql.toString() );
    }

    /**
     * Execute all queries
     */
    void run() throws SQLException
    {
        for ( TestData query : queries )
        {
            System.out.println( "\n*** " + query.name + " ***\n" );
            if ( query.sql != null )
            {
                System.out.println( query.sql );
                System.out.println( executeSql( query.sql ) );
            }
            System.out.println( query.cypher );
            System.out.println( executeCypher( query.cypher ) );
        }
    }

    void shutdown() throws SQLException
    {
        sqldb.close();
        graphdb.shutdown();
    }

    String executeCypher( String cypher )
    {
        return engine.execute( cypher )
                .toString();
    }

    String executeSql( String sql ) throws SQLException
    {
        StringBuilder builder = new StringBuilder( 512 );
        Statement statement = sqldb.createStatement();
        if ( statement.execute( sql ) )
        {
            ResultSet result = statement.getResultSet();
            ResultSetMetaData meta = result.getMetaData();
            int rowCount = 0;
            int columnCount = meta.getColumnCount();
            String line = new String( new char[columnCount] ).replace( "\0",
                    "+" + LINE_SEGMENT ) + "+\n";
            builder.append( line );

            for ( int i = 1; i <= columnCount; i++ )
            {
                String output = meta.getColumnLabel( i );
                printColumn( builder, output );
            }
            builder.append( "|\n" )
                    .append( line );
            while ( result.next() )
            {
                rowCount++;
                for ( int i = 1; i <= columnCount; i++ )
                {
                    String output = result.getString( i );
                    printColumn( builder, output );
                }
                builder.append( "|\n" );
            }
            result.close();
            builder.append( line )
                    .append( rowCount )
                    .append( " rows\n" );
        }
        else
        {
            throw new RuntimeException( "Couldn't execute: " + sql );
        }
        statement.close();
        return builder.toString();
    }

    private void printColumn( StringBuilder builder, String value )
    {
        if ( value == null )
        {
            value = "<null>";
        }
        builder.append( "| " )
                .append( value )
                .append( SPACE_SEGMENT.substring( value.length() + 1 ) );
    }

    class TestData
    {
        String name;
        String sql;
        String cypher;
        String[] matchStrings;

        /**
         * Create a sql/cypher test.
         * 
         * @param name the name of the test
         * @param sql the sql query string
         * @param cypher the cypher query string
         * @param matchStrings strings that should exist in the query result
         */
        public TestData( String name, String sql, String cypher,
                String... matchStrings )
        {
            this.name = name;
            this.sql = sql;
            this.cypher = cypher;
            this.matchStrings = matchStrings;
        }
    }
}
