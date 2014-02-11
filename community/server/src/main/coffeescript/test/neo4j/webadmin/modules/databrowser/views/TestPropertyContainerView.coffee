
define ['lib/amd/Backbone','neo4j/webadmin/modules/databrowser/views/PropertyContainerView'], (Backbone, PropertyContainerView) ->

  # TODO: Refactor out the "shouldBeConvertedToString" into it's own class
  describe "PropertyContainerView", ->  
    pcv = new PropertyContainerView(template:null)
    
    it "recognizes ascii characters as strings", ->
      expect(pcv.shouldBeConvertedToString "a").toBe(true)
      expect(pcv.shouldBeConvertedToString "abcd123 ").toBe(true)

    it "recognizes strings containing odd characters as strings", ->
      expect(pcv.shouldBeConvertedToString ";åäö #$ asd  ").toBe(true)

    it "recognizes valid JSON values as not being strings", ->
      expect(pcv.shouldBeConvertedToString "1").toBe(false)
      expect(pcv.shouldBeConvertedToString "12").toBe(false)
      expect(pcv.shouldBeConvertedToString "12.523").toBe(false)

      expect(pcv.shouldBeConvertedToString "['1','2','3']").toBe(false)
      expect(pcv.shouldBeConvertedToString "[1,2,3]").toBe(false)

      expect(pcv.shouldBeConvertedToString '"a quoted string"').toBe(false)
