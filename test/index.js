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

    testCollection.once('changeHint', (hint) => {

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

  lab.test('replace', (done) => {

    r1.b = 3;
    testCollection.put(r1)
    .then((result) => {

      expect(result.b).to.equal(3);
      return testCollection.get(r1.id);
    })
    .then((result) => {

      expect(result.b).to.equal(3);
      done();
    });
  });

  lab.test('update', (done) => {

    r1.b = 3;
    testCollection.update(r1.id, { b: 4 })
    .then((result) => {

      expect(result.b).to.equal(4);
      return testCollection.get(r1.id);
    })
    .then((result) => {

      expect(result.b).to.equal(4);
      return testCollection.size();
    })
    .then((result) => {

      expect(result).to.equal(2);
      done();
    });
  });

  lab.test('delete', (done) => {

    let hintCount = 0;
    const hintFunc = (hint) => {

      hintCount++;
      if (hintCount === 2) {
        testCollection.removeListener('changeHint', hintFunc);
      }
    };
    testCollection.on('changeHint', hintFunc);

    testCollection.find()
    .then((results) => {

      expect(results.length).to.equal(2);
      return testCollection.delete(results[0].id);
    })
    .then(() => {

      return testCollection.list();
    })
    .then((results) => {

      expect(results.length).to.equal(1);
      return testCollection.delete({ a: results[0].a, b: results[0].b });
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
