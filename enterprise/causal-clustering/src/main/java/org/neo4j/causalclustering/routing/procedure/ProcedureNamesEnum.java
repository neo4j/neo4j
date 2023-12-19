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
package org.neo4j.causalclustering.routing.procedure;

import java.util.Arrays;
import static java.lang.String.join;

public interface ProcedureNamesEnum
{
     String[] procedureNameSpace();
     String procedureName();

     default String[] fullyQualifiedProcedureName()
     {
         String[] namespace = procedureNameSpace();
         String[] fullyQualifiedProcedureName = Arrays.copyOf( namespace, namespace.length + 1 );
         fullyQualifiedProcedureName[namespace.length] = procedureName();
         return fullyQualifiedProcedureName;
     }

     default String callName()
     {
         return join( ".", procedureNameSpace()) + "." + procedureName();
     }

}
