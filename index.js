'use strict';

const Wadofgum = require('Wadofgum');
const WadofgumValidation = require('Wadofgum-validation');
const WadofgumProcess = require('Wadofgum-process');
const WadofgumKeyMap = require('Wadofgum-keymap');
const pg = require('pg-promise')();
const qrm = pg.queryResult;
const pkg = require('root-require')('./package.json');
const versuf = String.prototype.split.call(pkg.version, '.').join('x');

module.exports = (config) => {

  const db = pg(config.db);
  const createQuery = new pg.QueryFile('./sql/createCollection.sql');
  const whereFunc = `GOALPOST_WHERECLAUSEv${versuf}`;
  const findFunc = `GOALPOST_FINDv${versuf}`;
  const putFunc = `GOALPOST_PUTv${versuf}`;
  const postFunc = `GOALPOST_POSTv${versuf}`;
  const destroyFunc = `GOALPOST_DESTROYv${versuf}`;
  const getFunc = `GOALPOST_GETv${versuf}`;
  const deleteFunc = `GOALPOST_DELETEv${versuf}`;
  const deleteWhereFunc = `GOALPOST_DELETEWHEREv${versuf}`;

  class Collection extends Wadofgum.mixin(WadofgumValidation, WadofgumProcess, WadofgumKeyMap) {

    constructor(opts) {

      super(opts);
      this.name = opts.name;

      /* $lab:coverage:off$ */
      this.exists = opts.exists || false;
      this.primary = opts.primary || 'id';
      /* $lab:coverage:on$ */
    }

    _ready() {

      if (this.exists) {
        return Promise.resolve(db);
      }
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

    destroy() {

      return db.func(destroyFunc, [this.name, `pk_${this.name}_seq`]);
    }

    put(obj) {

      return this._ready()
      .then(() => {

        if (obj[this.primary]) {
          return db.func(putFunc, [this.name, obj[this.primary], obj], qrm.one);
        }
        return db.func(postFunc, [this.name, obj], qrm.one);
      })
      .then((results) => {

        const result = results.doc;
        result[this.primary] = results.id;
        return result;
      });
    }

    delete(input) {

      return this._ready()
      .then(() => {

        if (typeof input === 'object') {
          return db.func(deleteWhereFunc, [this.name, input])
          .then((res) => {

            return res;
          });
        }
        return db.func(deleteFunc, [this.name, input])
        .then((res) => {

          return;
        });
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

        return db.func(getFunc, [this.name, id], qrm.one);
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

        return db.func(findFunc, [this.name, opts]);
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
    funcs: {
      where: whereFunc,
      find: findFunc
    }
  };
};
