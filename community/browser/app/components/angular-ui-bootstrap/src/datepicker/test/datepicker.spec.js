describe('datepicker directive', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.datepicker'));
  beforeEach(module('template/datepicker/datepicker.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.date = new Date("September 30, 2010 15:30:00");
    element = $compile('<datepicker ng-model="date"></datepicker>')($rootScope);
    $rootScope.$digest();
  }));

  function getTitle() {
    return element.find('th').eq(1).find('button').first().text();
  }

  function clickTitleButton(times) {
    var el = element.find('th').eq(1).find('button');
    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function clickPreviousButton(times) {
    var el = element.find('th').eq(0).find('button').eq(0);
    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function clickNextButton(times) {
    var el = element.find('th').eq(2).find('button').eq(0);

    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function getLabelsRow() {
    return element.find('thead').find('tr').eq(1);
  }

  function getLabels() {
    var els = getLabelsRow().find('th');

    var labels = [];
    for (var i = 1, n = els.length; i < n; i++) {
      labels.push( els.eq(i).text() );
    }
    return labels;
  }

  function getWeeks() {
    var rows = element.find('tbody').find('tr');
    var weeks = [];
    for (var i = 0, n = rows.length; i < n; i++) {
      weeks.push( rows.eq(i).find('td').eq(0).first().text() );
    }
    return weeks;
  }

  function getOptions(rowIndex) {
    var cols = element.find('tbody').find('tr').eq(rowIndex).find('td');
    var days = [];
    for (var i = 1, n = cols.length; i < n; i++) {
      days.push( cols.eq(i).find('button').text() );
    }
    return days;
  }

  function getOptionsEl(rowIndex, colIndex) {
    return element.find('tbody').find('tr').eq(rowIndex).find('td').eq(colIndex + 1);
  }

  it('is a `<table>` element', function() {
    expect(element.prop('tagName')).toBe('TABLE');
    expect(element.find('thead').find('tr').length).toBe(2);
  });

  it('shows the correct title', function() {
    expect(getTitle()).toBe('September 2010');
  });

  it('shows the label row & the correct day labels', function() {
    expect(getLabelsRow().css('display')).not.toBe('none');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
  });

  it('renders the calendar days correctly', function() {
    expect(getOptions(0)).toEqual(['29', '30', '31', '01', '02', '03', '04']);
    expect(getOptions(1)).toEqual(['05', '06', '07', '08', '09', '10', '11']);
    expect(getOptions(2)).toEqual(['12', '13', '14', '15', '16', '17', '18']);
    expect(getOptions(3)).toEqual(['19', '20', '21', '22', '23', '24', '25']);
    expect(getOptions(4)).toEqual(['26', '27', '28', '29', '30', '01', '02']);
  });

  it('renders the week numbers correctly', function() {
    expect(getWeeks()).toEqual(['35', '36', '37', '38', '39']);
  });

  it('value is correct', function() {
    expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
  });

  it('has `selected` only the correct day', function() {
    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( ( i === 4 && j === 4) );
      }
    }
  });

  it('has no `selected` day when model is nulled', function() {
    $rootScope.date = null;
    $rootScope.$digest();

    expect($rootScope.date).toBe(null);

    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( false );
      }
    }
  });

  it('`disables` visible dates from other months', function() {
    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').find('span').hasClass('muted')).toBe( ((i === 0 && j < 3) || (i === 4 && j > 4)) );
      }
    }
  });

  it('updates the model when a day is clicked', function() {
    var el = getOptionsEl(2, 3).find('button');
    el.click();
    expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
  });

  it('moves to the previous month & renders correctly when `previous` button is clicked', function() {
    clickPreviousButton();

    expect(getTitle()).toBe('August 2010');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    expect(getOptions(0)).toEqual(['01', '02', '03', '04', '05', '06', '07']);
    expect(getOptions(1)).toEqual(['08', '09', '10', '11', '12', '13', '14']);
    expect(getOptions(2)).toEqual(['15', '16', '17', '18', '19', '20', '21']);
    expect(getOptions(3)).toEqual(['22', '23', '24', '25', '26', '27', '28']);
    expect(getOptions(4)).toEqual(['29', '30', '31', '01', '02', '03', '04']);

    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( false );
      }
    }
  });

  it('updates the model only when when a day is clicked in the `previous` month', function() {
    clickPreviousButton();
    expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));

    getOptionsEl(2, 3).find('button').click();
    expect($rootScope.date).toEqual(new Date('August 18, 2010 15:30:00'));
  });

  it('moves to the next month & renders correctly when `next` button is clicked', function() {
    clickNextButton();

    expect(getTitle()).toBe('October 2010');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    expect(getOptions(0)).toEqual(['26', '27', '28', '29', '30', '01', '02']);
    expect(getOptions(1)).toEqual(['03', '04', '05', '06', '07', '08', '09']);
    expect(getOptions(2)).toEqual(['10', '11', '12', '13', '14', '15', '16']);
    expect(getOptions(3)).toEqual(['17', '18', '19', '20', '21', '22', '23']);
    expect(getOptions(4)).toEqual(['24', '25', '26', '27', '28', '29', '30']);

    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( (i === 0 && j === 4) );
      }
    }
  });

  it('updates the model only when when a day is clicked in the `next` month', function() {
    clickNextButton();
    expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));

    getOptionsEl(2, 3).find('button').click();
    expect($rootScope.date).toEqual(new Date('October 13, 2010 15:30:00'));
  });

  it('updates the calendar when a day of another month is selected', function() {
    getOptionsEl(4, 5).find('button').click();
    expect($rootScope.date).toEqual(new Date('October 01, 2010 15:30:00'));
    expect(getTitle()).toBe('October 2010');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    expect(getOptions(0)).toEqual(['26', '27', '28', '29', '30', '01', '02']);
    expect(getOptions(1)).toEqual(['03', '04', '05', '06', '07', '08', '09']);
    expect(getOptions(2)).toEqual(['10', '11', '12', '13', '14', '15', '16']);
    expect(getOptions(3)).toEqual(['17', '18', '19', '20', '21', '22', '23']);
    expect(getOptions(4)).toEqual(['24', '25', '26', '27', '28', '29', '30']);

    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( (i === 0 && j === 5) );
      }
    }
  });

  it('updates calendar when `model` changes', function() {
    $rootScope.date = new Date('November 7, 2005 23:30:00');
    $rootScope.$digest();

    expect(getTitle()).toBe('November 2005');
    expect(getOptions(0)).toEqual(['30', '31', '01', '02', '03', '04', '05']);
    expect(getOptions(1)).toEqual(['06', '07', '08', '09', '10', '11', '12']);
    expect(getOptions(2)).toEqual(['13', '14', '15', '16', '17', '18', '19']);
    expect(getOptions(3)).toEqual(['20', '21', '22', '23', '24', '25', '26']);
    expect(getOptions(4)).toEqual(['27', '28', '29', '30', '01', '02', '03']);

    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( (i === 1 && j === 1) );
      }
    }
  });

  it('loops between different modes', function() {
    expect(getTitle()).toBe('September 2010');

    clickTitleButton();
    expect(getTitle()).toBe('2010');

    clickTitleButton();
    expect(getTitle()).toBe('2001 - 2020');

    clickTitleButton();
    expect(getTitle()).toBe('September 2010');
  });

  describe('month selection mode', function () {
    beforeEach(function() {
      clickTitleButton();
    });

    it('shows the year as title', function() {
      expect(getTitle()).toBe('2010');
    });

    it('shows months as options', function() {
      expect(getLabels()).toEqual([]);
      expect(getOptions(0)).toEqual(['January', 'February', 'March']);
      expect(getOptions(1)).toEqual(['April', 'May', 'June']);
      expect(getOptions(2)).toEqual(['July', 'August', 'September']);
      expect(getOptions(3)).toEqual(['October', 'November', 'December']);
    });

    it('does not change the model', function() {
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
    });

    it('has `selected` only the correct month', function() {
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( ( i === 2 && j === 2) );
        }
      }
    });

    it('moves to the previous year when `previous` button is clicked', function() {
      clickPreviousButton();

      expect(getTitle()).toBe('2009');
      expect(getLabels()).toEqual([]);
      expect(getOptions(0)).toEqual(['January', 'February', 'March']);
      expect(getOptions(1)).toEqual(['April', 'May', 'June']);
      expect(getOptions(2)).toEqual(['July', 'August', 'September']);
      expect(getOptions(3)).toEqual(['October', 'November', 'December']);

      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( false );
        }
      }
    });

    it('moves to the next year when `next` button is clicked', function() {
      clickNextButton();

      expect(getTitle()).toBe('2011');
      expect(getLabels()).toEqual([]);
      expect(getOptions(0)).toEqual(['January', 'February', 'March']);
      expect(getOptions(1)).toEqual(['April', 'May', 'June']);
      expect(getOptions(2)).toEqual(['July', 'August', 'September']);
      expect(getOptions(3)).toEqual(['October', 'November', 'December']);

      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( false );
        }
      }
    });

    it('renders correctly when a month is clicked', function() {
      clickPreviousButton(5);
      expect(getTitle()).toBe('2005');

      var monthNovEl = getOptionsEl(3, 1).find('button');
      monthNovEl.click();
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
      expect(getTitle()).toBe('November 2005');
      expect(getOptions(0)).toEqual(['30', '31', '01', '02', '03', '04', '05']);
      expect(getOptions(1)).toEqual(['06', '07', '08', '09', '10', '11', '12']);
      expect(getOptions(2)).toEqual(['13', '14', '15', '16', '17', '18', '19']);
      expect(getOptions(3)).toEqual(['20', '21', '22', '23', '24', '25', '26']);
      expect(getOptions(4)).toEqual(['27', '28', '29', '30', '01', '02', '03']);
    });
  });

  describe('year selection mode', function () {
    beforeEach(function() {
      clickTitleButton(2);
    });

    it('shows the year range as title', function() {
      expect(getTitle()).toBe('2001 - 2020');
    });

    it('shows years as options', function() {
      expect(getLabels()).toEqual([]);
      expect(getOptions(0)).toEqual(['2001', '2002', '2003', '2004', '2005']);
      expect(getOptions(1)).toEqual(['2006', '2007', '2008', '2009', '2010']);
      expect(getOptions(2)).toEqual(['2011', '2012', '2013', '2014', '2015']);
      expect(getOptions(3)).toEqual(['2016', '2017', '2018', '2019', '2020']);
    });

    it('does not change the model', function() {
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
    });

    it('has `selected` only the selected year', function() {
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 5; j ++) {
          expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( ( i === 1 && j === 4) );
        }
      }
    });

    it('moves to the previous year set when `previous` button is clicked', function() {
      clickPreviousButton();

      expect(getTitle()).toBe('1981 - 2000');
      expect(getLabels()).toEqual([]);
      expect(getOptions(0)).toEqual(['1981', '1982', '1983', '1984', '1985']);
      expect(getOptions(1)).toEqual(['1986', '1987', '1988', '1989', '1990']);
      expect(getOptions(2)).toEqual(['1991', '1992', '1993', '1994', '1995']);
      expect(getOptions(3)).toEqual(['1996', '1997', '1998', '1999', '2000']);

      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 5; j ++) {
          expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( false );
        }
      }
    });

    it('moves to the next year set when `next` button is clicked', function() {
      clickNextButton();

      expect(getTitle()).toBe('2021 - 2040');
      expect(getLabels()).toEqual([]);
      expect(getOptions(0)).toEqual(['2021', '2022', '2023', '2024', '2025']);
      expect(getOptions(1)).toEqual(['2026', '2027', '2028', '2029', '2030']);
      expect(getOptions(2)).toEqual(['2031', '2032', '2033', '2034', '2035']);
      expect(getOptions(3)).toEqual(['2036', '2037', '2038', '2039', '2040']);

      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 5; j ++) {
          expect(getOptionsEl(i, j).find('button').hasClass('btn-info')).toBe( false );
        }
      }
    });
  });

  describe('attribute `starting-day`', function () {
    beforeEach(function() {
      element = $compile('<datepicker ng-model="date" starting-day="1"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('shows the day labels rotated', function() {
      expect(getLabels()).toEqual(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']);
    });

    it('renders the calendar days correctly', function() {
      expect(getOptions(0)).toEqual(['30', '31', '01', '02', '03', '04', '05']);
      expect(getOptions(1)).toEqual(['06', '07', '08', '09', '10', '11', '12']);
      expect(getOptions(2)).toEqual(['13', '14', '15', '16', '17', '18', '19']);
      expect(getOptions(3)).toEqual(['20', '21', '22', '23', '24', '25', '26']);
      expect(getOptions(4)).toEqual(['27', '28', '29', '30', '01', '02', '03']);
    });

    it('renders the week numbers correctly', function() {
      expect(getWeeks()).toEqual(['35', '36', '37', '38', '39']);
    });
  });

  describe('attribute `show-weeks`', function () {
    var weekHeader, weekElement;
    beforeEach(function() {
      $rootScope.showWeeks = false;
      element = $compile('<datepicker ng-model="date" show-weeks="showWeeks"></datepicker>')($rootScope);
      $rootScope.$digest();

      weekHeader = getLabelsRow().find('th').eq(0);
      weekElement = element.find('tbody').find('tr').eq(1).find('td').eq(0);
    });

    it('hides week numbers based on variable', function() {
      expect(weekHeader.text()).toEqual('#');
      expect(weekHeader.css('display')).toBe('none');
      expect(weekElement.css('display')).toBe('none');
    });

    it('toggles week numbers', function() {
      $rootScope.showWeeks = true;
      $rootScope.$digest();
      expect(weekHeader.text()).toEqual('#');
      expect(weekHeader.css('display')).not.toBe('none');
      expect(weekElement.css('display')).not.toBe('none');

      $rootScope.showWeeks = false;
      $rootScope.$digest();
      expect(weekHeader.text()).toEqual('#');
      expect(weekHeader.css('display')).toBe('none');
      expect(weekElement.css('display')).toBe('none');
    });
  });

  describe('min attribute', function () {
    beforeEach(function() {
      $rootScope.mindate = new Date("September 12, 2010");
      element = $compile('<datepicker ng-model="date" min="mindate"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('disables appropriate days in current month', function() {
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( (i < 2) );
        }
      }
    });

    it('disables appropriate days when min date changes', function() {
      $rootScope.mindate = new Date("September 5, 2010");
      $rootScope.$digest();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( (i < 1) );
        }
      }
    });

    it('disables all days in previous month', function() {
      clickPreviousButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( true );
        }
      }
    });

    it('disables no days in next month', function() {
      clickNextButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( false );
        }
      }
    });

    it('disables appropriate months in current year', function() {
      clickTitleButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( (i < 2 || (i === 2 && j < 2)) );
        }
      }
    });

    it('disables all months in previous year', function() {
      clickTitleButton();
      clickPreviousButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( true );
        }
      }
    });

    it('disables no months in next year', function() {
      clickTitleButton();
      clickNextButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( false );
        }
      }
    });
  });

  describe('max attribute', function () {
    beforeEach(function() {
      $rootScope.maxdate = new Date("September 25, 2010");
      element = $compile('<datepicker ng-model="date" max="maxdate"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('disables appropriate days in current month', function() {
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( (i === 4) );
        }
      }
    });

    it('disables appropriate days when max date changes', function() {
      $rootScope.maxdate = new Date("September 18, 2010");
      $rootScope.$digest();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( (i > 2) );
        }
      }
    });

    it('disables no days in previous month', function() {
      clickPreviousButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( false );
        }
      }
    });

    it('disables all days in next month', function() {
      clickNextButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( true );
        }
      }
    });

    it('disables appropriate months in current year', function() {
      clickTitleButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( (i > 2 || (i === 2 && j > 2)) );
        }
      }
    });

    it('disables no months in previous year', function() {
      clickTitleButton();
      clickPreviousButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( false );
        }
      }
    });

    it('disables all months in next year', function() {
      clickTitleButton();
      clickNextButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(getOptionsEl(i, j).find('button').prop('disabled')).toBe( true );
        }
      }
    });
  });

  describe('date-disabled expression', function () {
    beforeEach(function() {
      $rootScope.dateDisabledHandler = jasmine.createSpy('dateDisabledHandler');
      element = $compile('<datepicker ng-model="date" date-disabled="dateDisabledHandler(date, mode)"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('executes the dateDisabled expression for each visible date', function() {
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(35);
    });

    it('executes the dateDisabled expression for each visible date & each month when mode changes', function() {
      clickTitleButton();
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(35 + 12);
    });

    it('executes the dateDisabled expression for each visible date, month & year when mode changes', function() {
      clickTitleButton(2);
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(35 + 12 + 20);
    });
  });

  describe('formatting attributes', function () {
    beforeEach(function() {
      element = $compile('<datepicker ng-model="date" day-format="\'d\'" day-header-format="\'EEEE\'" day-title-format="\'MMMM, yy\'" month-format="\'MMM\'" month-title-format="\'yy\'" year-format="\'yy\'" year-range="10"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('changes the title format in `day` mode', function() {
      expect(getTitle()).toBe('September, 10');
    });

    it('changes the title & months format in `month` mode', function() {
      clickTitleButton();

      expect(getTitle()).toBe('10');
      expect(getOptions(0)).toEqual(['Jan', 'Feb', 'Mar']);
      expect(getOptions(1)).toEqual(['Apr', 'May', 'Jun']);
      expect(getOptions(2)).toEqual(['Jul', 'Aug', 'Sep']);
      expect(getOptions(3)).toEqual(['Oct', 'Nov', 'Dec']);
    });

    it('changes the title, year format & range in `year` mode', function() {
      clickTitleButton(2);

      expect(getTitle()).toBe('01 - 10');
      expect(getOptions(0)).toEqual(['01', '02', '03', '04', '05']);
      expect(getOptions(1)).toEqual(['06', '07', '08', '09', '10']);
    });

    it('shows day labels', function() {
      expect(getLabels()).toEqual(['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']);
    });

    it('changes the day format', function() {
      expect(getOptions(0)).toEqual(['29', '30', '31', '1', '2', '3', '4']);
      expect(getOptions(1)).toEqual(['5', '6', '7', '8', '9', '10', '11']);
      expect(getOptions(4)).toEqual(['26', '27', '28', '29', '30', '1', '2']);
    });
  });

  describe('setting datepickerConfig', function() {
    var originalConfig = {};
    beforeEach(inject(function(_$compile_, _$rootScope_, datepickerConfig) {
      angular.extend(originalConfig, datepickerConfig);
      datepickerConfig.startingDay = 6;
      datepickerConfig.showWeeks = false;
      datepickerConfig.dayFormat = 'd';
      datepickerConfig.monthFormat = 'MMM';
      datepickerConfig.yearFormat = 'yy';
      datepickerConfig.yearRange = 10;
      datepickerConfig.dayHeaderFormat = 'EEEE';
      datepickerConfig.dayTitleFormat = 'MMMM, yy';
      datepickerConfig.monthTitleFormat = 'yy';

      element = $compile('<datepicker ng-model="date"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));
    afterEach(inject(function(datepickerConfig) {
      // return it to the original state
      angular.extend(datepickerConfig, originalConfig);
    }));

    it('changes the title format in day mode', function() {
      expect(getTitle()).toBe('September, 10');
    });

    it('changes the title & months format in `month` mode', function() {
      clickTitleButton();

      expect(getTitle()).toBe('10');
      expect(getOptions(0)).toEqual(['Jan', 'Feb', 'Mar']);
      expect(getOptions(1)).toEqual(['Apr', 'May', 'Jun']);
      expect(getOptions(2)).toEqual(['Jul', 'Aug', 'Sep']);
      expect(getOptions(3)).toEqual(['Oct', 'Nov', 'Dec']);
    });

    it('changes the title, year format & range in `year` mode', function() {
      clickTitleButton(2);

      expect(getTitle()).toBe('01 - 10');
      expect(getOptions(0)).toEqual(['01', '02', '03', '04', '05']);
      expect(getOptions(1)).toEqual(['06', '07', '08', '09', '10']);
    });

    it('changes the `starting-day` & day headers & format', function() {
      expect(getLabels()).toEqual(['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']);
      expect(getOptions(0)).toEqual(['28', '29', '30', '31', '1', '2', '3']);
      expect(getOptions(1)).toEqual(['4', '5', '6', '7', '8', '9', '10']);
      expect(getOptions(4)).toEqual(['25', '26', '27', '28', '29', '30', '1']);
    });

    it('changes initial visibility for weeks', function() {
      expect(getLabelsRow().find('th').eq(0).css('display')).toBe('none');
      for (var i = 0; i < 5; i++) {
        expect(element.find('tbody').find('tr').eq(i).find('td').eq(0).css('display')).toBe('none');
      }
    });

  });
});

describe('datepicker directive with empty initial state', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.datepicker'));
  beforeEach(module('template/datepicker/datepicker.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.date = null;
    element = $compile('<datepicker ng-model="date"></datepicker>')($rootScope);
    $rootScope.$digest();
  }));

  it('is a `<table>` element', function() {
    expect(element.prop('tagName')).toBe('TABLE');
    expect(element.find('thead').find('tr').length).toBe(2);
  });

  it('is shows rows with days', function() {
    expect(element.find('tbody').find('tr').length).toBeGreaterThan(3);
  });
});