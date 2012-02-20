deps = {}

require ["ribcage/ui/Nano"], (Nano) ->
  deps.loaded = true
  deps.Nano = Nano

describe "Nano", ->
  it "replaces placeholders", ->
    waitsFor -> deps.loaded
    runs ->
      n = deps.Nano
      expect(n.compile "{a}", {a:'b'}).toBe('b')
      expect(n.compile "{a.b}", {a:{b:'c'}}).toBe('c')
      expect(n.compile "{a.b.x}", {a:{b:'c'}}).toBe('N/A')
  it "allows truncating values", ->
    waitsFor -> deps.loaded
    runs ->
      n = deps.Nano
      expect(n.compile "{a|truncate:1}", {a:'1234'}).toBe('1')
