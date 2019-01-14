/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.shell;

import org.junit.Test;

import org.neo4j.SchemaHelper;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;

import static org.neo4j.graphdb.Label.label;

public class PECListingIT extends AbstractShellIT
{
    @Override
    protected GraphDatabaseAPI newDb()
    {
        return (GraphDatabaseAPI) new TestEnterpriseGraphDatabaseFactory()
                .setFileSystem( fs.get() )
                .newImpermanentDatabase();
    }

    @Test
    public void canListNodePropertyExistenceConstraints() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, "name" );

        // WHEN / THEN
        executeCommand( "schema ls", "ON \\(person:Person\\) ASSERT exists\\(person.name\\)" );
    }

    @Test
    public void canListRelationshipPropertyExistenceConstraints() throws Exception
    {
        // GIVEN
        RelationshipType relType = RelationshipType.withName( "KNOWS" );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType, "since" );

        // WHEN / THEN
        executeCommand( "schema ls", "ON \\(\\)-\\[knows:KNOWS\\]-\\(\\) ASSERT exists\\(knows.since\\)" );
    }

    @Test
    public void canListNodePropertyExistenceConstraintsByLabel() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, "name" );

        // WHEN / THEN
        executeCommand( "schema ls -l :Person", "ON \\(person:Person\\) ASSERT exists\\(person.name\\)" );
    }

    @Test
    public void canListRelationshipPropertyExistenceConstraintsByType() throws Exception
    {
        // GIVEN
        RelationshipType relType = RelationshipType.withName( "KNOWS" );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType, "since" );

        // WHEN / THEN
        executeCommand( "schema ls -r :KNOWS", "ON \\(\\)-\\[knows:KNOWS\\]-\\(\\) ASSERT exists\\(knows.since\\)" );
    }

    @Test
    public void canListRelationshipPropertyExistenceConstraintsByTypeAndProperty() throws Exception
    {
        // GIVEN
        RelationshipType relType = RelationshipType.withName( "KNOWS" );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType, "since" );

        // WHEN / THEN
        executeCommand( "schema ls -r :KNOWS -p since",
                "ON \\(\\)-\\[knows:KNOWS\\]-\\(\\) ASSERT exists\\(knows.since\\)" );
    }

    @Test
    public void canListBothNodeAndRelationshipPropertyExistenceConstraints() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        RelationshipType relType = RelationshipType.withName( "KNOWS" );

        // WHEN
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, "name" );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType, "since" );

        // THEN
        executeCommand( "schema ls",
                "ON \\(person:Person\\) ASSERT exists\\(person.name\\)",
                "ON \\(\\)-\\[knows:KNOWS\\]-\\(\\) ASSERT exists\\(knows.since\\)" );
    }

    @Test
    public void canListBothNodeAndRelationshipPropertyExistenceConstraintsByLabelAndType() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        RelationshipType relType = RelationshipType.withName( "KNOWS" );

        // WHEN
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, "name" );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType, "since" );

        // THEN
        executeCommand( "schema ls -l :Person -r :KNOWS",
                "ON \\(person:Person\\) ASSERT exists\\(person.name\\)",
                "ON \\(\\)-\\[knows:KNOWS\\]-\\(\\) ASSERT exists\\(knows.since\\)" );
    }

    @Test
    public void shouldHaveCorrectIndentationsInSchemaListing() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        RelationshipType relType = RelationshipType.withName( "KNOWS" );

        // WHEN
        SchemaHelper.createUniquenessConstraint( db, label, "name" );
        SchemaHelper.createNodeKeyConstraint( db, label, "surname" );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, "name" );
        SchemaHelper.createRelPropertyExistenceConstraint( db, relType, "since" );

        SchemaHelper.awaitIndexes( db );

        // THEN
        executeCommand( "schema",
                "Indexes",
                "  ON :Person\\(name\\)    ONLINE \\(for uniqueness constraint\\)",
                "  ON :Person\\(surname\\) ONLINE \\(for uniqueness constraint\\)",
                "Constraints",
                "  ON \\(person:Person\\) ASSERT person.name IS UNIQUE",
                "  ON \\(person:Person\\) ASSERT person.surname IS NODE KEY",
                "  ON \\(person:Person\\) ASSERT exists\\(person.name\\)",
                "  ON \\(\\)-\\[knows:KNOWS\\]-\\(\\) ASSERT exists\\(knows.since\\)" );
    }

    @Test
    public void canListNodePropertyExistenceConstraintsByLabelAndProperty() throws Exception
    {
        // GIVEN
        Label label = label( "Person" );
        SchemaHelper.createNodePropertyExistenceConstraint( db, label, "name" );

        // WHEN / THEN
        executeCommand( "schema ls -l :Person -p name", "ON \\(person:Person\\) ASSERT exists\\(person.name\\)" );
    }
}
