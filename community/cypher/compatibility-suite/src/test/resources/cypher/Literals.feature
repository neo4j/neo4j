#
# Copyright (c) 2002-2017 "Neo Technology,"
# Network Engine for Objects in Lund AB [http://neotechnology.com]
#
# This file is part of Neo4j.
#
# Neo4j is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

Feature: Literals

  Background:
    Given any graph

  Scenario: Returning an integer
    When executing query: RETURN 1 AS literal
    Then the result should be:
      | literal |
      | 1       |

  Scenario: Returning a float
    When executing query: RETURN 1.0 AS literal
    Then the result should be:
      | literal |
      | 1.0     |

  Scenario: Returning a float in exponent form
    When executing query: RETURN -1e-9 AS literal
    Then the result should be:
      | literal     |
      | -.000000001 |

  Scenario: Returning a boolean`
    When executing query: RETURN true AS literal
    Then the result should be:
      | literal |
      | true    |

  Scenario: Returning a single-quoted string
    When executing query: RETURN '' AS literal
    Then the result should be:
      | literal |
      | ''      |

  Scenario: Returning a double-quoted string
    When executing query: RETURN "" AS literal
    Then the result should be:
      | literal |
      | ''      |

  Scenario: Returning null
    When executing query: RETURN null AS literal
    Then the result should be:
      | literal |
      | null    |

  Scenario: Returning an empty list
    When executing query: RETURN [] AS literal
    Then the result should be:
      | literal |
      | []      |

  Scenario: Returning a nonempty list
    When executing query: RETURN [0,1,2] AS literal
    Then the result should be:
      | literal   |
      | [0, 1, 2] |

  Scenario: Returning an empty map
    When executing query: RETURN {} AS literal
    Then the result should be:
      | literal |
      | {}      |

  Scenario: Returning a nonempty map
    When executing query: RETURN {k1: 0, k2: "string"} AS literal
    Then the result should be:
      | literal               |
      | {k1: 0, k2: 'string'} |
