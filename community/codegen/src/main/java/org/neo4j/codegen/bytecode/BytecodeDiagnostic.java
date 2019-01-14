/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.codegen.bytecode;

import java.util.Locale;
import javax.tools.Diagnostic;

class BytecodeDiagnostic implements Diagnostic<Void>
{
    private final String message;

    BytecodeDiagnostic( String message )
    {
        this.message = message;
    }

    @Override
    public String getMessage( Locale locale )
    {
        return message;
    }

    @Override
    public Kind getKind()
    {
        return Kind.ERROR;
    }

    @Override
    public Void getSource()
    {
        return null;
    }

    @Override
    public long getPosition()
    {
        return NOPOS;
    }

    @Override
    public long getStartPosition()
    {
        return NOPOS;
    }

    @Override
    public long getEndPosition()
    {
        return NOPOS;
    }

    @Override
    public long getLineNumber()
    {
        return NOPOS;
    }

    @Override
    public long getColumnNumber()
    {
        return NOPOS;
    }

    @Override
    public String getCode()
    {
        return null;
    }
}
