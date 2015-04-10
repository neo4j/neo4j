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

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Deque;

import org.neo4j.driver.internal.messaging.MessageFormat;
import org.neo4j.driver.internal.messaging.Message;
import org.neo4j.driver.internal.messaging.PackStreamMessageFormatV1;

/**
 * Exposes a {@link org.neo4j.driver.internal.messaging.MessageFormat} as something Apache HTTP Client can consume.
 */
public class Neo4jMessagesEntity implements HttpEntity
{
    private static final BasicHeader CONTENT_TYPE =
            new BasicHeader( HttpHeaders.CONTENT_TYPE, PackStreamMessageFormatV1.CONTENT_TYPE );
    private final MessageFormat.Writer writer;

    private Deque<Message> messages;

    public Neo4jMessagesEntity( MessageFormat format )
    {
        writer = format.newWriter();
    }

    public Neo4jMessagesEntity reset( Deque<Message> messages )
    {
        this.messages = messages;
        return this;
    }

    @Override
    public boolean isRepeatable()
    {
        return false;
    }

    @Override
    public boolean isChunked()
    {
        return true;
    }

    @Override
    public long getContentLength()
    {
        return -1;
    }

    @Override
    public Header getContentType()
    {
        return CONTENT_TYPE;
    }

    @Override
    public Header getContentEncoding()
    {
        return null;
    }

    @Override
    public InputStream getContent() throws IOException, IllegalStateException
    {
        throw new IllegalStateException( "Content cannot be extracted as an input stream." );
    }

    @Override
    public void writeTo( OutputStream outstream ) throws IOException
    {
        writer.reset( outstream );
        Message msg;
        while ( (msg = messages.poll()) != null )
        {
            writer.write( msg );
        }
        writer.flush();
    }

    @Override
    public boolean isStreaming()
    {
        return true;
    }

    @Override
    public void consumeContent() throws IOException
    {
    }
}
