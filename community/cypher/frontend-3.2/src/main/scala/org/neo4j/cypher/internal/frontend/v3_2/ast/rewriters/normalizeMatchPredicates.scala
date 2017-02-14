package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

case object normalizeMatchPredicates
  extends MatchPredicateNormalization(MatchPredicateNormalizerChain(PropertyPredicateNormalizer, LabelPredicateNormalizer))
