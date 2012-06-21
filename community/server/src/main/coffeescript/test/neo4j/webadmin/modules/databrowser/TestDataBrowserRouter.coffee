

define ['lib/amd/Backbone',"neo4j/webadmin/modules/databrowser/DataBrowserRouter"], (Backbone, DataBrowserRouter) ->

  describe "DataBrowserRouter", ->
    it "can pick out read-only queries", ->
      dbr = new DataBrowserRouter(getServer:()->null)

      expect(dbr._looksLikeReadOnlyQuery "1").toBe(true)
      expect(dbr._looksLikeReadOnlyQuery "node:1").toBe(true)
      expect(dbr._looksLikeReadOnlyQuery "start n=node(0) return n").toBe(true)

      expect(dbr._looksLikeReadOnlyQuery "start n=node(0) match n--a return n").toBe(false)
      expect(dbr._looksLikeReadOnlyQuery "create n return n").toBe(false)
      expect(dbr._looksLikeReadOnlyQuery "start n=node(0) relate n--a return n").toBe(false)
      expect(dbr._looksLikeReadOnlyQuery "start n=node(0) set n.name='bob' return n").toBe(false)
