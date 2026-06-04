# Cypher Planner

## Integration tests
When creating integration tests, which inherit `LogicalPlanningAttributesTestSupport`, try to assert on the whole plan instead of matching on parts of it, so that it is clear for the reader of the test what the plan would look like.

A good example:

```scala
  test("should not plan node index scan with existence constraint if query has updates before the match") {
    val planner = plannerBuilder()
      .setAllNodesCardinality(1000)
      .setLabelCardinality("Label", 1000)
      .addNodeIndex("Label", Seq("prop"), 1.0, 1.0)
      .addNodeExistenceConstraint("Label", "prop")
      .build()

    val query =
      """CREATE (a:Label)
        |WITH count(*) AS c
        |MATCH (n:Label)
        |WHERE n.prop IS NULL
        |SET n.prop = 123""".stripMargin

    val plan = planner.plan(query).stripProduceResults
    plan shouldEqual planner.subPlanBuilder()
      .emptyResult()
      .setNodeProperty("n", "prop", "123")
      .filter("n.prop IS NULL")
      .apply()
      .|.nodeByLabelScan("n", "Label", "c")
      .aggregation(Seq(), Seq("count(*) AS c"))
      .create(createNode("a", "Label"))
      .argument()
      .build()
  }
```