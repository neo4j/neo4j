/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.procedure.builtin.graphschema;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.condition.EnabledIf;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.util.FeatureToggles;

class IntrospectSmokeIT {

    public static final String EXPECTED_FULL_RESULT =
            """
		{
		  "graphSchemaRepresentation" : {
		    "graphSchema" : {
		      "nodeLabels" : [ {
		        "$id" : "nl:A",
		        "token" : "A"
		      }, {
		        "$id" : "nl:B",
		        "token" : "B"
		      }, {
		        "$id" : "nl:L1",
		        "token" : "L1"
		      }, {
		        "$id" : "nl:L `2",
		        "token" : "`L ``2`"
		      }, {
		        "$id" : "nl:L2",
		        "token" : "L2"
		      }, {
		        "$id" : "nl:L3",
		        "token" : "L3"
		      }, {
		        "$id" : "nl:Unrelated",
		        "token" : "Unrelated"
		      }, {
		        "$id" : "nl:B1",
		        "token" : "B1"
		      }, {
		        "$id" : "nl:A1",
		        "token" : "A1"
		      }, {
		        "$id" : "nl:B2",
		        "token" : "B2"
		      }, {
		        "$id" : "nl:A2",
		        "token" : "A2"
		      }, {
		        "$id" : "nl:Book",
		        "token" : "Book"
		      }, {
		        "$id" : "nl:Actor",
		        "token" : "Actor"
		      }, {
		        "$id" : "nl:Person",
		        "token" : "Person"
		      }, {
		        "$id" : "nl:SomeNode",
		        "token" : "SomeNode"
		      } ],
		      "relationshipTypes" : [ {
		        "$id" : "rt:REVIEWED",
		        "token" : "REVIEWED"
		      }, {
		        "$id" : "rt:RELATED_TO",
		        "token" : "RELATED_TO"
		      }, {
		        "$id" : "rt:A_TYPE",
		        "token" : "A_TYPE"
		      }, {
		        "$id" : "rt:`WEIRD_TYPE",
		        "token" : "```WEIRD_TYPE`"
		      } ],
		      "nodeObjectTypes" : [ {
		        "$id" : "n:A1",
		        "labels" : [ {
		          "$ref" : "#nl:A1"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:A2",
		        "labels" : [ {
		          "$ref" : "#nl:A2"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:A",
		        "labels" : [ {
		          "$ref" : "#nl:A"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:Actor:Person",
		        "labels" : [ {
		          "$ref" : "#nl:Actor"
		        }, {
		          "$ref" : "#nl:Person"
		        } ],
		        "properties" : [ {
		          "token" : "name",
		          "type" : {
		            "type" : "string"
		          },
		          "nullable" : false
		        }, {
		          "token" : "id",
		          "type" : [ {
		            "type" : "array",
		            "items" : {
		              "type" : "integer"
		            }
		          }, {
		            "type" : "array",
		            "items" : {
		              "type" : "string"
		            }
		          }, {
		            "type" : "integer"
		          }, {
		            "type" : "string"
		          }, {
		            "type" : "array",
		            "items" : {
		              "type" : "float"
		            }
		          } ],
		          "nullable" : true
		        }, {
		          "token" : "f",
		          "type" : [ {
		            "type" : "integer"
		          }, {
		            "type" : "float"
		          } ],
		          "nullable" : true
		        }, {
		          "token" : "p",
		          "type" : {
		            "type" : "point"
		          },
		          "nullable" : true
		        } ]
		      }, {
		        "$id" : "n:B1",
		        "labels" : [ {
		          "$ref" : "#nl:B1"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:B2",
		        "labels" : [ {
		          "$ref" : "#nl:B2"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:B",
		        "labels" : [ {
		          "$ref" : "#nl:B"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:Book",
		        "labels" : [ {
		          "$ref" : "#nl:Book"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:L `2:L1",
		        "labels" : [ {
		          "$ref" : "#nl:L `2"
		        }, {
		          "$ref" : "#nl:L1"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:L1:L2",
		        "labels" : [ {
		          "$ref" : "#nl:L1"
		        }, {
		          "$ref" : "#nl:L2"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:L2:L3",
		        "labels" : [ {
		          "$ref" : "#nl:L2"
		        }, {
		          "$ref" : "#nl:L3"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:Person",
		        "labels" : [ {
		          "$ref" : "#nl:Person"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:SomeNode",
		        "labels" : [ {
		          "$ref" : "#nl:SomeNode"
		        } ],
		        "properties" : [ {
		          "token" : "idx",
		          "type" : {
		            "type" : "integer"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "n:Unrelated",
		        "labels" : [ {
		          "$ref" : "#nl:Unrelated"
		        } ],
		        "properties" : [ ]
		      } ],
		      "relationshipObjectTypes" : [ {
		        "$id" : "r:A_TYPE",
		        "type" : {
		          "$ref" : "#rt:A_TYPE"
		        },
		        "from" : {
		          "$ref" : "#n:A1"
		        },
		        "to" : {
		          "$ref" : "#n:B1"
		        },
		        "properties" : [ {
		          "token" : "x",
		          "type" : {
		            "type" : "string"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:A_TYPE_1",
		        "type" : {
		          "$ref" : "#rt:A_TYPE"
		        },
		        "from" : {
		          "$ref" : "#n:A2"
		        },
		        "to" : {
		          "$ref" : "#n:B2"
		        },
		        "properties" : [ {
		          "token" : "x",
		          "type" : {
		            "type" : "string"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:RELATED_TO",
		        "type" : {
		          "$ref" : "#rt:RELATED_TO"
		        },
		        "from" : {
		          "$ref" : "#n:L `2:L1"
		        },
		        "to" : {
		          "$ref" : "#n:L2:L3"
		        },
		        "properties" : [ {
		          "token" : "since",
		          "type" : {
		            "type" : "datetime"
		          },
		          "nullable" : false
		        }, {
		          "token" : "since",
		          "type" : {
		            "type" : "datetime"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:RELATED_TO_1",
		        "type" : {
		          "$ref" : "#rt:RELATED_TO"
		        },
		        "from" : {
		          "$ref" : "#n:L1:L2"
		        },
		        "to" : {
		          "$ref" : "#n:Unrelated"
		        },
		        "properties" : [ {
		          "token" : "since",
		          "type" : {
		            "type" : "datetime"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:REVIEWED",
		        "type" : {
		          "$ref" : "#rt:REVIEWED"
		        },
		        "from" : {
		          "$ref" : "#n:Person"
		        },
		        "to" : {
		          "$ref" : "#n:Book"
		        },
		        "properties" : [ ]
		      }, {
		        "$id" : "r:`WEIRD_TYPE",
		        "type" : {
		          "$ref" : "#rt:`WEIRD_TYPE"
		        },
		        "from" : {
		          "$ref" : "#n:A"
		        },
		        "to" : {
		          "$ref" : "#n:B"
		        },
		        "properties" : [ ]
		      } ]
		    }
		  }
		}""";
    public static final String EXPECTED_SAMPLED_RESULT =
            """
		{
		  "graphSchemaRepresentation" : {
		    "graphSchema" : {
		      "nodeLabels" : [ {
		        "$id" : "nl:A",
		        "token" : "A"
		      }, {
		        "$id" : "nl:B",
		        "token" : "B"
		      }, {
		        "$id" : "nl:L1",
		        "token" : "L1"
		      }, {
		        "$id" : "nl:L `2",
		        "token" : "`L ``2`"
		      }, {
		        "$id" : "nl:L2",
		        "token" : "L2"
		      }, {
		        "$id" : "nl:L3",
		        "token" : "L3"
		      }, {
		        "$id" : "nl:Unrelated",
		        "token" : "Unrelated"
		      }, {
		        "$id" : "nl:B1",
		        "token" : "B1"
		      }, {
		        "$id" : "nl:A1",
		        "token" : "A1"
		      }, {
		        "$id" : "nl:B2",
		        "token" : "B2"
		      }, {
		        "$id" : "nl:A2",
		        "token" : "A2"
		      }, {
		        "$id" : "nl:Book",
		        "token" : "Book"
		      }, {
		        "$id" : "nl:Actor",
		        "token" : "Actor"
		      }, {
		        "$id" : "nl:Person",
		        "token" : "Person"
		      }, {
		        "$id" : "nl:SomeNode",
		        "token" : "SomeNode"
		      } ],
		      "relationshipTypes" : [ {
		        "$id" : "rt:REVIEWED",
		        "token" : "REVIEWED"
		      }, {
		        "$id" : "rt:RELATED_TO",
		        "token" : "RELATED_TO"
		      }, {
		        "$id" : "rt:A_TYPE",
		        "token" : "A_TYPE"
		      }, {
		        "$id" : "rt:`WEIRD_TYPE",
		        "token" : "```WEIRD_TYPE`"
		      } ],
		      "nodeObjectTypes" : [ {
		        "$id" : "n:A1",
		        "labels" : [ {
		          "$ref" : "#nl:A1"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:A2",
		        "labels" : [ {
		          "$ref" : "#nl:A2"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:A",
		        "labels" : [ {
		          "$ref" : "#nl:A"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:Actor:Person",
		        "labels" : [ {
		          "$ref" : "#nl:Actor"
		        }, {
		          "$ref" : "#nl:Person"
		        } ],
		        "properties" : [ {
		          "token" : "name",
		          "type" : {
		            "type" : "string"
		          },
		          "nullable" : false
		        }, {
		          "token" : "id",
		          "type" : [ {
		            "type" : "array",
		            "items" : {
		              "type" : "integer"
		            }
		          }, {
		            "type" : "array",
		            "items" : {
		              "type" : "string"
		            }
		          }, {
		            "type" : "integer"
		          }, {
		            "type" : "string"
		          }, {
		            "type" : "array",
		            "items" : {
		              "type" : "float"
		            }
		          } ],
		          "nullable" : true
		        }, {
		          "token" : "f",
		          "type" : [ {
		            "type" : "integer"
		          }, {
		            "type" : "float"
		          } ],
		          "nullable" : true
		        }, {
		          "token" : "p",
		          "type" : {
		            "type" : "point"
		          },
		          "nullable" : true
		        } ]
		      }, {
		        "$id" : "n:B1",
		        "labels" : [ {
		          "$ref" : "#nl:B1"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:B2",
		        "labels" : [ {
		          "$ref" : "#nl:B2"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:B",
		        "labels" : [ {
		          "$ref" : "#nl:B"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:Book",
		        "labels" : [ {
		          "$ref" : "#nl:Book"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:L `2:L1",
		        "labels" : [ {
		          "$ref" : "#nl:L `2"
		        }, {
		          "$ref" : "#nl:L1"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:L1:L2",
		        "labels" : [ {
		          "$ref" : "#nl:L1"
		        }, {
		          "$ref" : "#nl:L2"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:L2:L3",
		        "labels" : [ {
		          "$ref" : "#nl:L2"
		        }, {
		          "$ref" : "#nl:L3"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:Person",
		        "labels" : [ {
		          "$ref" : "#nl:Person"
		        } ],
		        "properties" : [ ]
		      }, {
		        "$id" : "n:SomeNode",
		        "labels" : [ {
		          "$ref" : "#nl:SomeNode"
		        } ],
		        "properties" : [ {
		          "token" : "idx",
		          "type" : {
		            "type" : "integer"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "n:Unrelated",
		        "labels" : [ {
		          "$ref" : "#nl:Unrelated"
		        } ],
		        "properties" : [ ]
		      } ],
		      "relationshipObjectTypes" : [ {
		        "$id" : "r:A_TYPE",
		        "type" : {
		          "$ref" : "#rt:A_TYPE"
		        },
		        "from" : {
		          "$ref" : "#n:A1"
		        },
		        "to" : {
		          "$ref" : "#n:B1"
		        },
		        "properties" : [ {
		          "token" : "x",
		          "type" : {
		            "type" : "string"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:RELATED_TO",
		        "type" : {
		          "$ref" : "#rt:RELATED_TO"
		        },
		        "from" : {
		          "$ref" : "#n:L `2:L1"
		        },
		        "to" : {
		          "$ref" : "#n:L2:L3"
		        },
		        "properties" : [ {
		          "token" : "since",
		          "type" : {
		            "type" : "datetime"
		          },
		          "nullable" : false
		        }, {
		          "token" : "since",
		          "type" : {
		            "type" : "datetime"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:RELATED_TO_1",
		        "type" : {
		          "$ref" : "#rt:RELATED_TO"
		        },
		        "from" : {
		          "$ref" : "#n:L1:L2"
		        },
		        "to" : {
		          "$ref" : "#n:Unrelated"
		        },
		        "properties" : [ {
		          "token" : "since",
		          "type" : {
		            "type" : "datetime"
		          },
		          "nullable" : false
		        } ]
		      }, {
		        "$id" : "r:REVIEWED",
		        "type" : {
		          "$ref" : "#rt:REVIEWED"
		        },
		        "from" : {
		          "$ref" : "#n:Person"
		        },
		        "to" : {
		          "$ref" : "#n:Book"
		        },
		        "properties" : [ ]
		      }, {
		        "$id" : "r:`WEIRD_TYPE",
		        "type" : {
		          "$ref" : "#rt:`WEIRD_TYPE"
		        },
		        "from" : {
		          "$ref" : "#n:A"
		        },
		        "to" : {
		          "$ref" : "#n:B"
		        },
		        "properties" : [ ]
		      } ]
		    }
		  }
		}""";

    private static final String DATABASE_NAME = "neo4j";

    private static DatabaseManagementService managementService;

    @BeforeAll
    static void setUp() {

        synchronized (IntrospectSmokeIT.class) {
            FeatureToggles.set(Introspect.class, "enabled", true);
            managementService = new TestDatabaseManagementServiceBuilder()
                    .useLazyProcedures(false)
                    .impermanent()
                    .build();
        }

        var database = managementService.database(DATABASE_NAME);
        database.executeTransactionally("MATCH (n) DETACH DELETE n");
        database.executeTransactionally(
                """
					UNWIND range(1, 5) AS i
									WITH i CREATE (n:SomeNode {idx: i})
					""");
        database.executeTransactionally(
                """
			CREATE (:Actor:Person {name: 'Weird Id1', id: 'abc'})
			CREATE (:Actor:Person {name: 'Weird Id2', id: 4711})
			CREATE (:Actor:Person {name: 'Weird Id3', id: ["pt1", "pt2"]})
			CREATE (:Actor:Person {name: 'Weird Id4', id: [21, 23, 42]})
			CREATE (:Actor:Person {name: 'A compromise id', id: [0.5]})
			CREATE (:Actor:Person {name: 'A number', f: 0.5})
			CREATE (:Actor:Person {name: 'Another number', f: 50})
			CREATE (:Actor:Person {name: 'A point', p: point({latitude:toFloat('13.43'), longitude:toFloat('56.21')})})
			CREATE (:L1:`L ``2`) - [:RELATED_TO {since: datetime()}] -> (:L2:L3)
			CREATE (:L1:L2) - [:RELATED_TO {since: datetime()}] -> (:L2:L3)
			CREATE (:L1:L2) - [:RELATED_TO {since: datetime()}] -> (:Unrelated)
			CREATE (:Person) -[:REVIEWED] ->(:Book)
			CREATE (a:A) -[:```WEIRD_TYPE`] -> (:B)
			""");
        database.executeTransactionally("CREATE (:A1) -[:A_TYPE {x: 'hallo'}]-> (:B1)\n"
                        .repeat((int) (GraphSchema.Introspector.DEFAULT_SAMPLE_SIZE * 10))
                + "CREATE (:A2) -[:A_TYPE {x: 'hallo'}]-> (:B2)\n");
    }

    @AfterAll
    static void tearDown() {
        managementService.shutdown();
    }

    static boolean procedureWasLoadedSuccessfully() {

        return FeatureToggles.flag(Introspect.class, "enabled", false);
    }

    static Stream<Arguments> smokeTest() {
        return Stream.of(Arguments.of(EXPECTED_FULL_RESULT, false), Arguments.of(EXPECTED_SAMPLED_RESULT, true));
    }

    @ParameterizedTest(name = "sample only: {1}")
    @MethodSource
    @EnabledIf(
            value = "procedureWasLoadedSuccessfully",
            disabledReason = "Something went wrong while toggling the feature flag during this test")
    void smokeTest(String expected, boolean sampleOnly) {
        var result = managementService
                .database(DATABASE_NAME)
                .executeTransactionally(
                        "CALL internal.introspect.asJson({useConstantIds: true, prettyPrint: true, sampleOnly: $sampleOnly}) YIELD value RETURN value AS result",
                        Map.of("sampleOnly", sampleOnly),
                        r -> r.next().get("result"));

        // Windows is adding \r to newlines, but our expectation is \n.
        var sanitizedString = ((String) result).replace("\r\n", "\n");
        assertThat(sanitizedString).isEqualTo(expected);
    }
}
