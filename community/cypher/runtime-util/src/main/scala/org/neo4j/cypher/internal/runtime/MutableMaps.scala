package org.neo4j.cypher.internal.runtime

object MutableMaps {

  def create[KEY, VALUE](size: Int) : collection.mutable.Map[KEY, VALUE] =
    new collection.mutable.OpenHashMap[KEY, VALUE](if (size < 16) 16 else size)

  def empty[KEY, VALUE]: collection.mutable.Map[KEY, VALUE] = create(16)

  def create[KEY, VALUE](input: scala.collection.Map[KEY, VALUE]) : collection.mutable.Map[KEY, VALUE] =
    create(input.size) ++= input

  def create[KEY, VALUE](input: (KEY, VALUE)*) : collection.mutable.Map[KEY, VALUE] = {
    create(input.size) ++= input
  }
}
