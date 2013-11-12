describe 'Frames', ->
  it 'works', ->
    browser().navigateTo('/')
    expect(browser().location().url()).toBe('/');

