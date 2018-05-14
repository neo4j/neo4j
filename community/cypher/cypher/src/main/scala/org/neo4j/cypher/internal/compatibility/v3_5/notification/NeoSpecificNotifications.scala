package org.neo4j.cypher.internal.compatibility.v3_5.notification

import org.opencypher.v9_0.util.InternalNotification

// TODO
case class SuboptimalIndexForWildcardQueryNotification(label: String, propertyKeys: Seq[String])// extends InternalNotification