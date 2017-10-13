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
package org.neo4j.internal.store.prototype.neole;

import java.io.IOException;

import org.neo4j.internal.kernel.api.ExplicitIndexRead;
import org.neo4j.internal.kernel.api.ExplicitIndexWrite;
import org.neo4j.internal.kernel.api.Locks;
import org.neo4j.internal.kernel.api.Read;
import org.neo4j.internal.kernel.api.SchemaRead;
import org.neo4j.internal.kernel.api.SchemaWrite;
import org.neo4j.internal.kernel.api.Write;

public class NeoLETransaction implements org.neo4j.internal.kernel.api.Transaction
{
    private final ReadStore read;

    public NeoLETransaction( ReadStore read ) throws IOException
    {
        this.read = read;
    }

    @Override
    public long commit()
    {
        return 0;
    }

    @Override
    public void rollback()
    {

    }

    @Override
    public Read dataRead()
    {
        return read;
    }

    @Override
    public Write dataWrite()
    {
        return null;
    }

    @Override
    public ExplicitIndexRead indexRead()
    {
        return null;
    }

    @Override
    public ExplicitIndexWrite indexWrite()
    {
        return null;
    }

    @Override
    public SchemaRead schemaRead()
    {
        return null;
    }

    @Override
    public SchemaWrite schemaWrite()
    {
        return null;
    }

    @Override
    public Locks locks()
    {
        return null;
    }
}
