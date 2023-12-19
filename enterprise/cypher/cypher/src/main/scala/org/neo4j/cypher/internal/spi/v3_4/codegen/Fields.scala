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
package org.neo4j.cypher.internal.spi.v3_4.codegen

import org.neo4j.codegen.FieldReference

case class Fields(entityAccessor: FieldReference,
                  executionMode: FieldReference,
                  description: FieldReference,
                  tracer: FieldReference,
                  params: FieldReference,
                  closeable: FieldReference,
                  queryContext: FieldReference,
                  cursors: FieldReference,
                  nodeCursor: FieldReference,
                  relationshipScanCursor: FieldReference,
                  propertyCursor: FieldReference,
                  dataRead: FieldReference,
                  tokenRead: FieldReference,
                  schemaRead: FieldReference
                 )
