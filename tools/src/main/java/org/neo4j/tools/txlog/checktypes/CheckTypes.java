/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
