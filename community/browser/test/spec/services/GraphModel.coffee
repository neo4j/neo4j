'use strict'

describe 'Service: GraphModel', () ->
  # load the service's module
  beforeEach module 'neo4jApp.services'

  # instantiate service
  GraphModel = {}
  beforeEach inject (_GraphModel_) ->
    GraphModel = _GraphModel_

  createNode = (id) ->
    {
      "id": id
      "labels": []
      "properties": {}
    }

  createRelationship = (id, start, end, type) ->
    {
      "id": id
      "startNode": start
      "endNode": end
      "type": type
    }

  describe 'initializing a graph', ->
    it 'should store a node', ->
      graph = new GraphModel( { nodes: [createNode(1)], relationships: []})
      expect(graph.nodes().length).toBe 1
      expect(graph.nodes()[0].id).toBe 1

    it 'should store a relationship',  ->
      graph = new GraphModel( {nodes: [createNode(1), createNode(2)], relationships: [createRelationship(0, 1, 2)]})
      relationships = graph.relationships()
      expect(relationships.length).toBe 1
      expect(relationships[0].source.id).toBe 1
      expect(relationships[0].target.id).toBe 2

    it 'complains if relationship refers to non-existent node', ->
      expect(-> new GraphModel( {nodes: [], relationships: [createRelationship(0, 1, 2)]}))
        .toThrow("Malformed graph: must add nodes before relationships that connect them")

  describe 'addNode:', ->
    it 'should add a node to collection', ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      node = createNode(1)
      graph.addNode(node)
      expect(graph.nodes().length).toBe 1

    it 'should not add new node with existing id', ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      node = graph.addNode(createNode(1))
      graph.addNode(createNode(1))
      expect(graph.nodes().length).toBe 1
      expect(graph.nodes()[0]).toBe node

    it 'should be able to add a node with id 0', ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      node = createNode(0)
      graph.addNode(node)
      expect(graph.nodes().length).toBe 1

  describe 'adding relationship', ->
    it 'connects two nodes', ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      graph.addNode(createNode(1))
      graph.addNode(createNode(2))
      graph.addRelationship(createRelationship(0, 1, 2))
      relationships = graph.relationships()
      expect(relationships.length).toBe 1
      expect(relationships[0].source.id).toBe 1
      expect(relationships[0].target.id).toBe 2

    it "complains if one of the nodes doesn't exist", ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      graph.addNode(createNode(2))
      expect(-> graph.addRelationship(createRelationship(0, 1, 2)))
        .toThrow("Malformed graph: must add nodes before relationships that connect them")

  describe 'singleNode model', ->
    it 'change graphModel state to singleNode', ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      expect(graph.isSingleMode).toBe false
      graph.switchSingleMode true
      expect(graph.isSingleMode).toBe true

    it 'should save target node relation when in singleNode model', ->
      graph = new GraphModel( { nodes: [], relationships: [] })
      graph.switchSingleMode true
      graph.addNode(createNode(1))
      graph.addNode(createNode(3))
      graph.addNode(createNode(2))
      graph.addRelationship(createRelationship(0, 1, 3, 'like'))
      graph.addRelationship(createRelationship(0, 1, 3, 'not like'))
      graph.addRelationship(createRelationship(0, 1, 2, 'not like'))
      relationships = graph.relationships()
      expect(relationships.length).toBe 0
      types = graph.types()
      expect(types.length).toBe 2
      relationshipstype = graph.relationshiptypeMap
      expect(relationshipstype['not like'].length).toBe 2
