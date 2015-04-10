/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.driver.internal.connector.http;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.driver.Value;
import org.neo4j.driver.exceptions.DatabaseException;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.driver.internal.messaging.MessageHandler;
import org.neo4j.driver.internal.spi.StreamCollector;

public class HttpResponseHandler implements MessageHandler
{
    public static final String[] NO_FIELDS = new String[0];
    private final Map<Integer,StreamCollector> collectors = new HashMap<>();

    /** Counts number of responses, used to correlate response data with stream collectors */
    private int responseId = 0;

    /** If a failure occurs, the error gets stored here */
    private Neo4jException error;

    @Override
    public void handleRecordMessage( Value[] fields )
    {
        // This is not very efficient, using something like a singly-linked queue with a current collector as head
        // would take advantage of the ordered nature of exchanges and avoid all these objects allocated from boxing
        // below.
        StreamCollector collector = collectors.get( responseId );
        if ( collector != null )
        {
            collector.record( fields );
        }
    }

    @Override
    public void handleFailureMessage( String code, String message )
    {
        try
        {
            String[] parts = code.split( "\\." );
            String classification = parts[1];
            if ( classification.equals( "ClientError" ) )
            {
                error = new ClientException( code, message );
            }
            else if ( classification.equals( "TransientError" ) )
            {
                error = new TransientException( code, message );
            }
            else
            {
                error = new DatabaseException( code, message );
            }
        }
        finally
        {
            responseId++;
        }
    }

    @Override
    public void handleSuccessMessage( Map<String,Value> meta )
    {
        StreamCollector collector = collectors.get( responseId );
        if ( collector != null )
        {
            collector.fieldNames( fieldNamesFromMeta( meta ) );
        }
        responseId++;
    }

    @Override
    public void handleIgnoredMessage()
    {
        responseId++;
    }

    @Override
    public void handleDiscardAllMessage()
    {
    }

    @Override
    public void handleAckFailureMessage()
    {
    }

    @Override
    public void handlePullAllMessage()
    {
    }

    @Override
    public void handleRunMessage( String statement, Map<String,Value> parameters )
    {
    }

    public void registerResultCollector( int correlationId, StreamCollector collector )
    {
        collectors.put( correlationId, collector );
    }

    public boolean serverFailureOccurred()
    {
        return error != null;
    }

    public Neo4jException serverFailure()
    {
        return error;
    }

    public void clear()
    {
        responseId = 0;
        error = null;
        collectors.clear();
    }

    private String[] fieldNamesFromMeta( Map<String,Value> meta )
    {
        String[] fields;
        Value fieldValue = meta.get( "fields" );
        if ( fieldValue == null || fieldValue.size() == 0 )
        {
            fields = NO_FIELDS;
        }
        else
        {
            fields = new String[(int) fieldValue.size()];
            int idx = 0;
            for ( Value value : fieldValue )
            {
                fields[idx++] = value.javaString();
            }

        }
        return fields;
    }
}