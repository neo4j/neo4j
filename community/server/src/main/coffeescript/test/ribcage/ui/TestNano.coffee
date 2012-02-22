
define ["ribcage/ui/Nano"], (Nano) ->

  describe "Nano", ->
    it "replaces placeholders", ->
      expect(Nano.compile "{a}", {a:'b'}).toBe('b')
      expect(Nano.compile "{a.b}", {a:{b:'c'}}).toBe('c')
      expect(Nano.compile "{a.b.x}", {a:{b:'c'}}).toBe('N/A')
    it "allows truncating values", ->
      expect(Nano.compile "{a|truncate:1}", {a:'1234'}).toBe('1')
