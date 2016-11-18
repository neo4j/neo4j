/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.tools.dump;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.StringReader;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.FormattedLog;
import org.neo4j.tools.dump.InconsistencyReportReader.Inconsistencies;

import static org.junit.Assert.assertEquals;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.asSet;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.iterator;

public class InconsistencyReportReaderTest
{
    @Test
    public void shouldReadBasicEntities() throws Exception
    {
        // GIVEN
        ByteArrayOutputStream out = new ByteArrayOutputStream( 1_000 );
        FormattedLog log = FormattedLog.toOutputStream( out );
        InconsistencyMessageLogger logger = new InconsistencyMessageLogger( log );
        long nodeId = 5;
        long relationshipGroupId = 10;
        long relationshipId = 15;
        long propertyId = 20;
        logger.error( RecordType.NODE, new NodeRecord( nodeId ),
                "Some error", "something" );
        logger.error( RecordType.RELATIONSHIP, new RelationshipRecord( relationshipId ),
                "Some error", "something" );
        logger.error( RecordType.RELATIONSHIP_GROUP, new RelationshipGroupRecord( relationshipGroupId ),
                "Some error", "something" );
        logger.error( RecordType.PROPERTY, new PropertyRecord( propertyId ),
                "Some error", "something" );
        String text = out.toString();
        PrimitiveLongSet nodes = Primitive.longSet();
        PrimitiveLongSet relationships = Primitive.longSet();
        PrimitiveLongSet relationshipGroups = Primitive.longSet();
        PrimitiveLongSet properties = Primitive.longSet();

        // WHEN
        InconsistencyReportReader reader = new InconsistencyReportReader( new Inconsistencies()
        {
            @Override
            public void relationshipGroup( long id )
            {
                relationshipGroups.add( id );
            }

            @Override
            public void relationship( long id )
            {
                relationships.add( id );
            }

            @Override
            public void property( long id )
            {
                properties.add( id );
            }

            @Override
            public void node( long id )
            {
                nodes.add( id );
            }
        } );
        reader.read( new StringReader( text ) );

        // THEN
        assertEquals( asSet( iterator( nodeId ) ), nodes );
        assertEquals( asSet( iterator( relationshipId ) ), relationships );
        assertEquals( asSet( iterator( relationshipGroupId ) ), relationshipGroups );
        assertEquals( asSet( iterator( propertyId ) ), properties );
    }
}
