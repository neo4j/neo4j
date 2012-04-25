
define ["neo4j/webadmin/modules/databrowser/views/PropertyContainerView"], (PropertyContainerView) ->

  describe "PropertyContainerView", ->
    it "recognizes non-quoted strings as strings", ->
      pcv = new PropertyContainerView(template:null)

      expect(pcv.shouldBeConvertedToString "a").toBe(true)
      expect(pcv.shouldBeConvertedToString "åäö").toBe(true)
      expect(pcv.shouldBeConvertedToString "åäö #$ asd  ").toBe(true)
      expect(pcv.shouldBeConvertedToString ";åäö #$ asd  ").toBe(true)

      expect(pcv.shouldBeConvertedToString "1").toBe(false)
      expect(pcv.shouldBeConvertedToString "12").toBe(false)
      expect(pcv.shouldBeConvertedToString "12.523").toBe(false)

      expect(pcv.shouldBeConvertedToString "['1','2','3']").toBe(false)
      expect(pcv.shouldBeConvertedToString "[1,2,3]").toBe(false)

      expect(pcv.shouldBeConvertedToString '"a quoted string"').toBe(false)
