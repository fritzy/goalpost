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

    testCollection.once('updateHint', (hint) => {

      expect(hint.id).to.equal(id);
      expect(hint.type).to.equal('put');
      done();
    });

    const data = {
      a: 1,
      b: 2
    };

    testCollection.put(data)
    .then((r) => {

      id = r.id;
      r1 = r;
    });
  });

  lab.test('get', (done) => {

    testCollection.get(id)
    .then((r2) => {

      expect(Lodash.isEqual(r1, r2)).to.equal(true);
      done();
    });
  });

  lab.test('find', (done) => {


    testCollection.put({ a: 3, b: 4 })
    .then(() => {

      return testCollection.find({ where: { a: 1, b: 2 } });
    })
    .then((results) => {

      expect(Lodash.isEqual(r1, results[0])).to.equal(true);
      expect(results.length).to.equal(1);
      return testCollection.find();
    })
    .then((results) => {

      expect(results.length).to.equal(2);
      done();
    });
  });

  lab.test('delete', (done) => {

    let hintCount = 0;
    const hintFunc = (hint) => {

      hintCount++;
      if (hintCount === 2) {
        testCollection.removeListener('updateHint', hintFunc);
      }
    };
    testCollection.on('updateHint', hintFunc);

    testCollection.find()
    .then((results) => {

      expect(results.length).to.equal(2);
      return testCollection.delete(results[0].id);
    })
    .then(() => {

      return testCollection.find();
    })
    .then((results) => {

      expect(results.length).to.equal(1);
      return testCollection.delete(results[0].id);
    })
    .then((res) => {

      return testCollection.find();
    })
    .then((results) => {

      expect(hintCount).to.equal(2);
      expect(results.length).to.equal(0);
      done();
    });
  });

});
