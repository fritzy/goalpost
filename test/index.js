'use strict';

const lab = exports.lab = require('lab').script();
const expect = require('code').expect;
const Joi = require('joi');
const config = require('getconfig');
const lodash = require('lodash');

const GoalPost = require('../')(config);

process.on('uncaughtException', function (err) {
  console.log(err.stack);
});

process.on('unhandledRejection', function (err) {
  console.log(err.stack);
});

lab.experiment('basic put and get', () => {

  let testCollection = new GoalPost.Collection({
    name: 'test01'
  });

  lab.after((done) => {
    testCollection.destroy()
    .then(() => {
      done();
    });
  });

  lab.test('put', (done) => {

    const data = {
      a: 1,
      b: 2
    }
    let id;
    let r1;

    testCollection.put(data)
    .then((r) => {
      id = r.id;
      console.log(r)
      r1 = r;
      return testCollection.get(r.id)
    })
    .then((r2) => {
      console.log('x', r2);
      expect(lodash.isEqual(r1, r2)).to.equal(true);
      done();
    });
  });

  lab.test('find', (done) => {
    testCollection.find({where: {a: 1, b: 2}})
    .then((results) => {
      console.log(results);
      done();
    });
  });

});
