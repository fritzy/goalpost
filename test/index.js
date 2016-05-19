'use strict';

const Lodash = require('lodash');
const lab = exports.lab = require('lab').script();
const expect = require('code').expect;
const Config = require('getconfig');
const GoalPost = require('../')(Config);

process.on('uncaughtException', (err) => console.log(err.stack));
process.on('unhandledRejection', (err) => console.log(err.stack));

lab.experiment('basic put and get', () => {

  const testCollection = new GoalPost.Collection({
    name: 'test01'
  });

  lab.after((done) => {

    testCollection.destroy()
    .then(() => {

      done();
    });
  });

  let id;
  let r1;

  lab.test('put', (done) => {

    const data = {
      a: 1,
      b: 2
    };

    testCollection.put(data)
    .then((r) => {

      id = r.id;
      r1 = r;
      done();
    });
  });

  lab.test('get', (done) => {

    return testCollection.get(id)
    .then((r2) => {

      expect(Lodash.isEqual(r1, r2)).to.equal(true);
      done();
    });
  });

  lab.test('find', (done) => {

    testCollection.find({ where: { a: 1, b: 2 } })
    .then((results) => {

      done();
    });
  });

});
