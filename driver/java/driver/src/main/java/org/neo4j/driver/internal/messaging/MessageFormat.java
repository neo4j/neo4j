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
package org.neo4j.driver.internal.messaging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface MessageFormat
{
    interface Writer
    {
        void write( Message msg ) throws IOException;

        void flush() throws IOException;

        Writer reset( OutputStream channel );
    }

    interface Reader
    {
        /**
         * Return true is there is another message in the underlying buffer
         */
        boolean hasNext() throws IOException;

        void read( MessageHandler handler ) throws IOException;

        Reader reset( InputStream channel ) throws IOException;
    }

    Writer newWriter();

    Reader newReader();
}
