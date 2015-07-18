/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.integrationtest;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.test.DatabaseRule;

import static org.neo4j.graphdb.DynamicRelationshipType.withName;

public class MandatoryRelationshipPropertyConstrainVerificationIT extends MandatoryPropertyConstraintVerificationIT
{
    @Override
    public void createConstraint( DatabaseRule db, String relType, String property )
    {
        db.schema().constraintFor( withName( relType ) ).assertPropertyExists( property ).create();
    }

    @Override
    public String constraintCreationMethodName()
    {
        return "mandatoryRelationshipPropertyConstraintCreate";
    }

    @Override
    public long createOffender( DatabaseRule db, String key )
    {
        Node start = db.createNode();
        Node end = db.createNode();
        Relationship relationship = start.createRelationshipTo( end, withName( key ) );
        return relationship.getId();
    }

    @Override
    public String offenderCreationMethodName()
    {
        return "relationshipCreate"; // takes schema read lock to enforce constraints
    }

    @Override
    public Class<?> offenderType()
    {
        return Relationship.class;
    }
}
