package org.neo4j.cypher.internal.pipes

import org.scalatest.Assertions
import org.neo4j.cypher.internal.commands.values.LabelName
import org.junit.Test
import org.neo4j.cypher.PathImpl
import org.neo4j.graphdb.{Relationship, Node}
import org.scalatest.mock.MockitoSugar
import org.neo4j.cypher.internal.symbols.{CollectionType, StringType, LabelType}

class ResultValueMapperPipeTest extends Assertions with MockitoSugar {

  @Test
  def should_map_label_values_to_strings() {
    val fake = new FakePipe(Seq(Map("foo" -> LabelName("label"), "bar" -> 12)))
    val pipe = new ResultValueMapperPipe(fake)

    val result = pipe.createResults(QueryState())
    assert(result.toList === List(Map("foo" -> "label", "bar" -> 12)))
  }

  @Test
  def should_map_collection_values_as_well() {
    val fake = new FakePipe(Seq(Map("foo" -> Seq(LabelName("label"), LabelName("label2")))))
    val pipe = new ResultValueMapperPipe(fake)

    val result = pipe.createResults(QueryState())
    assert(result.toList === List(Map("foo" -> Seq("label", "label2"))))
  }

  @Test
  def should_not_touch_paths() {
    val node = mock[Node]

    val path = new PathImpl(node)
    val fake = new FakePipe(Seq(Map("foo" -> path)))
    val pipe = new ResultValueMapperPipe(fake)

    val result = pipe.createResults(QueryState())
    assert(result.toList === List(Map("foo" -> path)))
  }

  @Test
  def should_not_touch_nodes() {
    val node = mock[Node]

    val fake = new FakePipe(Seq(Map("foo" -> node)))
    val pipe = new ResultValueMapperPipe(fake)

    val result = pipe.createResults(QueryState())
    assert(result.toList === List(Map("foo" -> node)))
  }

  @Test
  def should_not_touch_relationships() {
    val rel = mock[Relationship]

    val fake = new FakePipe(Seq(Map("foo" -> rel)))
    val pipe = new ResultValueMapperPipe(fake)

    val result = pipe.createResults(QueryState())
    assert(result.toList === List(Map("foo" -> rel)))
  }

  @Test
  def should_change_identifier_types() {
    val fake = new FakePipe(Seq(), "x" -> LabelType(), "y" -> StringType())
    val pipe = new ResultValueMapperPipe(fake)

    assert(pipe.symbols.identifiers === Map("y" -> StringType(), "x" -> StringType()))
  }

  @Test
  def should_change_identifier_types_in_collections() {
    val fake = new FakePipe(Seq(), "x" -> new CollectionType(LabelType()))
    val pipe = new ResultValueMapperPipe(fake)

    assert(pipe.symbols.identifiers === Map("x" -> new CollectionType(StringType())))
  }
}