/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.procedure.builtin;

import java.util.function.IntFunction;

import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.internal.schema.ConstraintDescriptor;
import org.neo4j.internal.schema.ConstraintType;
import org.neo4j.internal.schema.SchemaDescriptor;

public class ConstraintsProcedureUtil
{
    static String prettyPrint( ConstraintDescriptor constraintDescriptor, TokenNameLookup tokenNameLookup )
    {
        SchemaDescriptor schema = constraintDescriptor.schema();
        int[] entityTokenIds = schema.getEntityTokenIds();
        if ( entityTokenIds.length != 1 )
        {
            throw new IllegalArgumentException( "Cannot pretty-print multi-token constraints: " + constraintDescriptor.userDescription( tokenNameLookup ) );
        }
        String entityTypeName = schema.entityType() == EntityType.NODE ? tokenNameLookup.labelGetName( entityTokenIds[0] ) :
                                tokenNameLookup.relationshipTypeGetName( entityTokenIds[0] );
        entityTypeName = escapeLabelOrRelTyp( entityTypeName );
        String entityName = entityTypeName.toLowerCase();
        String properties = formatProperties( schema.getPropertyIds(), tokenNameLookup, entityName );

        ConstraintType type = constraintDescriptor.type();
        switch ( type )
        {
        case EXISTS:
            switch ( schema.entityType() )
            {
            case NODE:
                return "CONSTRAINT ON ( " + entityName + ":" + entityTypeName + " ) ASSERT " + properties + " IS NOT NULL";
            case RELATIONSHIP:
                return "CONSTRAINT ON ()-[ " + entityName + ":" + entityTypeName + " ]-() ASSERT " + properties + " IS NOT NULL";
            default:
                throw new IllegalStateException( "Unknown schema entity type: " + schema.entityType() + "." );
            }
        case UNIQUE:
            return "CONSTRAINT ON ( " + entityName + ":" + entityTypeName + " ) ASSERT " + properties + " IS UNIQUE";
        case UNIQUE_EXISTS:
            return "CONSTRAINT ON ( " + entityName + ":" + entityTypeName + " ) ASSERT " + properties + " IS NODE KEY";
        default:
            throw new IllegalStateException( "Unknown constraint type: " + type + "." );
        }
    }

    private static String escapeLabelOrRelTyp( String name )
    {
        if ( name.contains( ":" ) )
        {
            return "`" + name + "`";
        }
        else
        {
            return name;
        }
    }

    private static String formatProperties( int[] propertyIds, TokenNameLookup tokenNameLookup, String nodeName )
    {
        return niceProperties( tokenNameLookup, propertyIds, nodeName + "." );
    }

    private static String niceProperties( TokenNameLookup lookup, int[] propertyIds, String prefix )
    {
        StringBuilder out = new StringBuilder();
        out.append( '(' );
        format( out, prefix, ", ", lookup::propertyKeyGetName, propertyIds );
        out.append( ')' );
        return out.toString();
    }

    private static void format( StringBuilder out, String prefix, String separator, IntFunction<String> lookup, int[] ids )
    {
        for ( int id : ids )
        {
            String name = lookup.apply( id );
            out.append( prefix );
            if ( name.contains( ":" ) )
            {
                out.append( '`' ).append( name ).append( '`' );
            }
            else
            {
                out.append( name );
            }
            out.append( separator );
        }
        if ( ids.length > 0 )
        {
            out.setLength( out.length() - separator.length() );
        }
    }
}
