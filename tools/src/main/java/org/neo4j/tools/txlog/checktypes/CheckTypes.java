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
package org.neo4j.tools.txlog.checktypes;

import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.transaction.command.Command;

public class CheckTypes
{
    public static final NodeCheckType NODE = new NodeCheckType();
    public static final PropertyCheckType PROPERTY = new PropertyCheckType();
    public static final RelationshipCheckType RELATIONSHIP = new RelationshipCheckType();
    public static final RelationshipGroupCheckType RELATIONSHIP_GROUP = new RelationshipGroupCheckType();
    public static final NeoStoreCheckType NEO_STORE = new NeoStoreCheckType();

    @SuppressWarnings( "unchecked" )
    public static final CheckType<? extends Command, ? extends AbstractBaseRecord>[] CHECK_TYPES =
            new CheckType[]{NODE, PROPERTY, RELATIONSHIP, RELATIONSHIP_GROUP, NEO_STORE};

    private CheckTypes()
    {
    }

    public static <C extends Command,R extends AbstractBaseRecord> CheckType<C,R> fromName( String name )
    {
        for ( CheckType<?,?> checkType : CHECK_TYPES )
        {
            if ( checkType.name().equals( name ) )
            {
                //noinspection unchecked
                return (CheckType<C,R>) checkType;
            }
        }
        throw new IllegalArgumentException( "Unknown check named " + name );
    }
}
