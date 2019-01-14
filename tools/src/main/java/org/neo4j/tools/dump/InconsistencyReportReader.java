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
package org.neo4j.tools.dump;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.neo4j.tools.dump.InconsistentRecords.Type;

/**
 * Reads CC inconsistency reports. Example of entry:
 * <p>
 * <pre>
 * ERROR: The referenced relationship record is not in use.
 *     Node[3496089,used=true,rel=14833798,prop=13305361,labels=Inline(0x1000000006:[6]),light,secondaryUnitId=-1]
 *     Inconsistent with: Relationship[14833798,used=false,source=0,target=0,type=0,sPrev=0,sNext=0,tPrev=0,tNext=0,
 *     prop=0,secondaryUnitId=-1,!sFirst,!tFirst]
 * </pre>
 * <p>
 * Another example entry:
 * <p>
 * <pre>
 * ERROR: The first outgoing relationship is not the first in its chain.
 *     RelationshipGroup[12144403,type=9,out=2988709379,in=-1,loop=-1,prev=-1,next=40467195,used=true,owner=635306933,secondaryUnitId=-1]
 * </pre>
 */
public class InconsistencyReportReader
{
    private static final String INCONSISTENT_WITH = "Inconsistent with: ";
    private final InconsistentRecords inconsistencies;

    public InconsistencyReportReader( InconsistentRecords inconsistencies )
    {
        this.inconsistencies = inconsistencies;
    }

    public void read( File file ) throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) )
        {
            read( reader );
        }
    }

    public void read( BufferedReader bufferedReader ) throws IOException
    {
        String line = bufferedReader.readLine();
        Type inconsistentRecordType;
        Type inconsistentWithRecordType;
        long inconsistentRecordId;
        long inconsistentWithRecordId;

        while ( line != null )
        {
            if ( line.contains( "ERROR" ) || line.contains( "WARNING" ) )
            {
                // The current line is the inconsistency description line.
                // Get the inconsistent entity line:
                line = bufferedReader.readLine();
                if ( line == null )
                {
                    return; // Unexpected end of report.
                }
                line = line.trim();
                inconsistentRecordType = toRecordType( entityType( line ) );
                if ( inconsistentRecordType == null )
                {
                    continue;
                }

                inconsistentRecordId = inconsistentRecordType.extractId( line );
                inconsistencies.reportInconsistency( inconsistentRecordType, inconsistentRecordId );

                // Then get the Inconsistent With line:
                line = bufferedReader.readLine();
                if ( line == null || !line.contains( INCONSISTENT_WITH ) )
                {
                    // There's no Inconsistent With line, so we report what we have.
                    inconsistencies.reportInconsistency( inconsistentRecordType, inconsistentRecordId );
                    // Leave the current line for the next iteration of the loop.
                }
                else
                {
                    line = line.substring( INCONSISTENT_WITH.length() ).trim();
                    inconsistentWithRecordType = toRecordType( entityType( line ) );
                    if ( inconsistentWithRecordType != null )
                    {
                        inconsistentWithRecordId = inconsistentWithRecordType.extractId( line );
                        inconsistencies.reportInconsistency( inconsistentWithRecordType, inconsistentWithRecordId );
                    }
                    line = bufferedReader.readLine(); // Prepare a line for the next iteration of the loop.
                }
            }
            else
            {
                // The current line doesn't fit with anything we were expecting to see, so we skip it and try the
                // next line.
                line = bufferedReader.readLine();
            }
        }
    }

    private Type toRecordType( String entityType )
    {
        if ( entityType == null )
        {
            // Skip unrecognizable lines.
            return null;
        }

        switch ( entityType )
        {
        case "Relationship":
            return Type.RELATIONSHIP;
        case "Node":
            return Type.NODE;
        case "Property":
            return Type.PROPERTY;
        case "RelationshipGroup":
            return Type.RELATIONSHIP_GROUP;
        case "IndexRule":
            return Type.SCHEMA_INDEX;
        case "IndexEntry":
            return Type.NODE;
        case "NodeLabelRange":
            return Type.NODE_LABEL_RANGE;
        default:
            // it's OK, we just haven't implemented support for this yet
            return null;
        }
    }

    private String entityType( String line )
    {
        int bracket = line.indexOf( '[' );
        return bracket == -1 ? null : line.substring( 0, bracket );
    }
}
