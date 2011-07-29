package org.neo4j.cypher.commands

class QueryBuilder(startItems: Seq[StartItem]) {
  var matching: Option[Match] = None
  var where: Option[Clause] = None
  var aggregation: Option[Aggregation] = None
  var orderBy: Option[Sort] = None
  var skip: Option[Int] = None
  var limit: Option[Int] = None

  def matches(patterns: Pattern*): QueryBuilder = store(() => matching = Some(Match(patterns: _*)))

  def where(clause: Clause): QueryBuilder = store(() => where = Some(clause))

  def aggregation(aggregationItems: AggregationItem*): QueryBuilder = store(() => aggregation = Some(Aggregation(aggregationItems: _*)))

  def orderBy(sortItems: SortItem*): QueryBuilder = store(() => orderBy = Some(Sort(sortItems: _*)))

  def skip(skipTo: Int): QueryBuilder = store(() => skip = Some(skipTo))

  def limit(limitTo: Int): QueryBuilder = store(() => limit = Some(limitTo))

  def slice: Option[Slice] = (skip, limit) match {
    case (None, None) => None
    case (s, l) => Some(Slice(skip, limit))
  }

  private def store(f: () => Unit): QueryBuilder = {
    f()
    this
  }

  def RETURN(returnItems: ReturnItem*): Query = Query(Return(returnItems: _*), Start(startItems: _*), matching, where, aggregation, orderBy, slice)
}