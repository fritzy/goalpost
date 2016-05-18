'use strict';

const wadofgum = require('wadofgum');
const wadofgumValidation = require('wadofgum-validation');
const wadofgumProcess = require('wadofgum-process');
const wadofgumKeyMap = require('wadofgum-keymap');
const pg = require('pg-promise')();
const lodash = require('lodash');
const qrm = pg.queryResult;
const pkg = require('root-require')('./package.json');
const versuf = String.prototype.split.call(pkg.version, '.').join('x');

module.exports = (config) => {

  const db = pg(config.db);
  const createQuery = new pg.QueryFile('./createCollection.sql');
  const putQuery = new pg.QueryFile('./putCollection.sql');
  const postQuery = new pg.QueryFile('./postCollection.sql');
  const destroyQuery = new pg.QueryFile('./destroyCollection.sql');
  const getQuery = new pg.QueryFile('./getCollection.sql', {debug: true});
  const spQuery = new pg.QueryFile('./spCollection.sql');

  class Collection extends wadofgum.mixin(wadofgumValidation, wadofgumProcess, wadofgumKeyMap) {

    constructor(opts) {

      super(opts);
      this.name = opts.name;
      this.exists = opts.exists || false;
      this.primary = opts.primary || 'id';
    }

    _ready() {
      if (this.exists) return Promise.resolve(db);
      return db.query(createQuery, [this.name, `idx_${this.name}_gin`, `pk_${this.name}_seq`, versuf])
      .then(() => {
        this.exists = true;
        return db;
      });
    }

    size(obj) {
      this._ready()
      .then(() => {
      });
    }
    /*
     * {where {$or: [{}]}
     */

    destroy() {
      return db.query(destroyQuery, [this.name, `pk_${this.name}_seq`])
    }

    put(obj) {
      return this._ready()
      .then((db) => {
        if (obj[this.primary]) 
          return db.query(putQuery, [this.name, obj[this.primary], obj], qrm.one);
        return db.query(postQuery, [this.name, obj], qrm.one);
      })
      .then((results) => {
        const result = results.doc;
        result[this.primary] = results.id;
        return result;
      });
    }

    delete(id) {
      return this._ready()
      .then(() => {
      });
    }

    update(obj) {
      return this._ready()
      .then(() => {
      });
    }

    get(id) {
      return this._ready()
      .then(() => {
        return db.query(getQuery, [this.name, id], qrm.one)
      })
      .then((results) => {
        const obj = results.doc;
        obj[this.primary] = results.id;
        return obj;
      });
    }

    find(opts) {
      return this._ready()
      .then(() => {
        console.log(JSON.stringify(opts))
        return db.func('GOALPOST_FINDv1x0x0', [this.name, opts])
      })
      .then((results) => {
        return results.map((result) => {
          const obj = result.doc;
          obj[this.primary] = result.id;
          return obj;
        });
      });
    }

    list(opts) {
      return find(opts);
    }

  }

  return {
    Collection,
  }
}
