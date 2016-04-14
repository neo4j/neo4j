package org.neo4j.cypher.internal.spi.v3_1.codegen

import org.neo4j.codegen.{FieldReference, MethodReference}

case class Fields(closer: FieldReference,
                          ro: FieldReference,
                          entityAccessor: FieldReference,
                          executionMode: FieldReference,
                          description: FieldReference,
                          tracer: FieldReference,
                          params: FieldReference,
                          closeable: FieldReference,
                          success: MethodReference,
                          close: MethodReference)
