/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.neo4j.cypher.internal.v3_5.expressions

import java.io.File

import org.apache.commons.codec.binary.Base64
import org.neo4j.blob.{BlobFactory, Blob}
import org.neo4j.cypher.internal.v3_5.util.InputPosition
import org.neo4j.cypher.internal.v3_5.{expressions => ast}

trait BlobURL {
  def asCanonicalString: String;

  def createBlob(): Blob;
}

case class BlobLiteralExpr(value: BlobURL)(val position: InputPosition)
  extends ast.Expression {
  override def asCanonicalStringVal = value.asCanonicalString
}

case class BlobFileURL(filePath: String) extends BlobURL {
  override def asCanonicalString = filePath

  def createBlob(): Blob = BlobFactory.fromFile(new File(filePath))
}

case class BlobBase64URL(base64: String) extends BlobURL {
  override def asCanonicalString = base64

  def createBlob(): Blob = BlobFactory.fromBytes(Base64.decodeBase64(base64))
}

case class BlobHttpURL(url: String) extends BlobURL {
  override def asCanonicalString = url

  def createBlob(): Blob = BlobFactory.fromHttpURL(url)
}

case class BlobFtpURL(url: String) extends BlobURL {
  override def asCanonicalString = url

  def createBlob(): Blob = BlobFactory.fromURL(url)
}