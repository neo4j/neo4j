//package org.neo4j.lab.cypher.pipes
//
///**
//* Created by Andres Taylor
//* Date: 4/20/11
//* Time: 08:37
//*/
//
//class RelatedToPipe(from: Pipe, filter: (Map[String, Any]) => Map[String, Any]) extends Pipe {
//  def foreach[U](f: (Map[String, Any]) => U) {
//    from.foreach((m) => {
//      m ++ f.apply(m)
//    })
//  }
//}