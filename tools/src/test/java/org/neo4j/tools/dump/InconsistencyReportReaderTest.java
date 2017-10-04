/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.StringReader;

import org.neo4j.consistency.RecordType;
import org.neo4j.consistency.report.InconsistencyMessageLogger;
import org.neo4j.consistency.store.synthetic.IndexEntry;
import org.neo4j.kernel.api.index.SchemaIndexProvider;
import org.neo4j.kernel.api.schema.index.IndexDescriptorFactory;
import org.neo4j.kernel.impl.store.record.IndexRule;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.logging.FormattedLog;
import org.neo4j.tools.dump.inconsistency.ReportInconsistencies;

import static org.junit.Assert.assertTrue;

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
                IndexRule.indexRule( indexId, IndexDescriptorFactory.forLabel( 1, 2 ),
                        new SchemaIndexProvider.Descriptor( "key", "version" ) ).toString() );
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
}
