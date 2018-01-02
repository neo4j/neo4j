/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FailingByteChannel extends KnownDataByteChannel
{
    private final String failWithMessage;
    private final int sizeToFailAt;

    public FailingByteChannel( int sizeToFailAt, String failWithMessage )
    {
        super( sizeToFailAt*2 );
        this.sizeToFailAt = sizeToFailAt;
        this.failWithMessage = failWithMessage;
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        if ( position > sizeToFailAt ) throw new MadeUpException( failWithMessage );
        return super.read( dst );
    }
}
