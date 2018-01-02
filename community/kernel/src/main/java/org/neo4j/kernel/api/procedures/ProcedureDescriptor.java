/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.api.procedures;

/** Describes a procedure stored in the database */
public class ProcedureDescriptor
{
    private final ProcedureSignature signature;
    private final String language;
    private final String procedureBody;

    public ProcedureDescriptor( ProcedureSignature signature, String language, String procedureBody )
    {
        this.signature = signature;
        this.language = language;
        this.procedureBody = procedureBody;
    }

    public ProcedureSignature signature()
    {
        return signature;
    }

    public String language()
    {
        return language;
    }

    public String procedureBody()
    {
        return procedureBody;
    }

    @Override
    public boolean equals( Object o )
    {
        // Note that equality is *only* checked on signature, this is probably wrong, but is depended on in
        // TxState. Please don't change this without modifying how txstate tracks changes to procedures.
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }

        ProcedureDescriptor that = (ProcedureDescriptor) o;

        return signature.equals( that.signature );

    }

    @Override
    public int hashCode()
    {
        return signature.hashCode();
    }
}