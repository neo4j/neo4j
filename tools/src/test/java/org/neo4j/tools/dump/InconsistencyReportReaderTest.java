/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.tools.dump;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.internal.kernel.api.schema.IndexProviderDescriptor;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.FormattedLog;
import org.neo4j.storageengine.api.schema.IndexDescriptorFactory;
import org.neo4j.tools.dump.inconsistency.ReportInconsistencies;

import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.api.schema.SchemaDescriptorFactory.forLabel;

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
        long indexNodeId = 7;
        long nodeNotInTheIndexId = 17;
        long indexId = 99;
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
        logger.error( RecordType.INDEX, new IndexEntry( indexNodeId ), "Some index error", "Something wrong with index" );
        logger.error( RecordType.NODE, new NodeRecord( nodeNotInTheIndexId ), "Some index error",
                      IndexDescriptorFactory.forSchema( forLabel( 1, 2 ),
                                              new IndexProviderDescriptor( "key", "version" ) ).withId( indexId ).toString() );
        String text = out.toString();

        // WHEN
        ReportInconsistencies inconsistencies = new ReportInconsistencies();
        InconsistencyReportReader reader = new InconsistencyReportReader( inconsistencies );
        reader.read( new BufferedReader( new StringReader( text ) ) );

        // THEN
        assertTrue( inconsistencies.containsNodeId( nodeId ) );
        assertTrue( inconsistencies.containsNodeId( indexNodeId ) );
        assertTrue( inconsistencies.containsNodeId( nodeNotInTheIndexId ) );
        assertTrue( inconsistencies.containsRelationshipId( relationshipId ) );
        assertTrue( inconsistencies.containsRelationshipGroupId( relationshipGroupId ) );
        assertTrue( inconsistencies.containsPropertyId( propertyId ) );
        assertTrue( inconsistencies.containsSchemaIndexId( indexId ) );
    }

    @Test
    public void shouldParseRelationshipGroupInconsistencies() throws Exception
    {
        // Given
        ReportInconsistencies inconsistencies = new ReportInconsistencies();
        String text =
                "ERROR: The first outgoing relationship is not the first in its chain.\n" +
                "\tRelationshipGroup[1337,type=1,out=2,in=-1,loop=-1,prev=-1,next=3,used=true,owner=4,secondaryUnitId=-1]\n" +
                "ERROR: The first outgoing relationship is not the first in its chain.\n" +
                "\tRelationshipGroup[4242,type=1,out=2,in=-1,loop=-1,prev=-1,next=3,used=true,owner=4,secondaryUnitId=-1]\n";

        // When
        InconsistencyReportReader reader = new InconsistencyReportReader( inconsistencies );
        reader.read( new BufferedReader( new StringReader( text ) ) );

        // Then
        assertTrue( inconsistencies.containsRelationshipGroupId( 1337 ) );
        assertTrue( inconsistencies.containsRelationshipGroupId( 4242 ) );
    }
}
