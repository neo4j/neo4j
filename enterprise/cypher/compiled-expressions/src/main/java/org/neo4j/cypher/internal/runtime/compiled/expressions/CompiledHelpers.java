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
package org.neo4j.cypher.internal.runtime.compiled.expressions;

import org.opencypher.v9_0.util.CypherTypeException;

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.values.AnyValue;
import org.neo4j.values.storable.BooleanValue;
import org.neo4j.values.storable.Value;

import static org.neo4j.values.storable.Values.NO_VALUE;

/**
 * Contains helper methods used from compiled expressions
 */
@SuppressWarnings( "unused" )
public final class CompiledHelpers
{
    private CompiledHelpers()
    {
        throw new UnsupportedOperationException( "do not instantiate" );
    }

    public static Value assertBooleanOrNoValue( AnyValue value )
    {
        if ( value != NO_VALUE && !(value instanceof BooleanValue ) )
        {
            throw new CypherTypeException( String.format( "Don't know how to treat a predicate: %s", value.toString() ),
                    null );
        }
        return (Value) value;
    }

    public static AnyValue loadVariable( ExecutionContext ctx, String name )
    {
        if ( !ctx.contains( name ) )
        {
            throw new NotFoundException( String.format( "Unknown variable `%s`.", name ) );
        }
        return ctx.apply( name );
    }
}
