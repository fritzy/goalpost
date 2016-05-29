'use strict';

const Wadofgum = require('Wadofgum');
const WadofgumValidation = require('Wadofgum-validation');
const WadofgumProcess = require('Wadofgum-process');
const WadofgumKeyMap = require('Wadofgum-keymap');
const EventEmitter = require('events').EventEmitter;
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
  const sizeofFunc = `GOALPOST_SIZEOFv${versuf}`;
  const updateFunc = `GOALPOST_UPDATEv${versuf}`;
  const destroyFunc = `GOALPOST_DESTROYv${versuf}`;
  const getFunc = `GOALPOST_GETv${versuf}`;
  const deleteFunc = `GOALPOST_DELETEv${versuf}`;
  const deleteWhereFunc = `GOALPOST_DELETEWHEREv${versuf}`;


  class Collection extends Wadofgum.mixin(WadofgumValidation, WadofgumProcess, WadofgumKeyMap) {

    constructor(opts) {

      super(opts);
      EventEmitter.call(this);
      this.listenClient = null;
      this.listener = null;
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

        return new Promise((resolve, reject) => {

          this.listenClient = new pg.pg.Client(config.db);
          this.listenClient.connect((err, client) => {

            this.listener = client;
            const hint = `goalpost_hint_${this.name}`;
            const full = `goalpost_full_${this.name}`;
            /* $lab:coverage:off$ */
            if (err) {
              return reject(err);
            }
            /* $lab:coverage:on$ */
            this.listener = client;
            client.on('notification', (msg) => {

              /* $lab:coverage:off$ */
              if (msg.name === 'notification') {
              /* $lab:coverage:on$ */
                const payload = JSON.parse(msg.payload);
                if (msg.channel === hint) {
                  this.emit('changeHint', payload);
                }
                else if (msg.channel === full) {
                  this.emit('changeFull', payload);
                }
              }
            });
            client.query(`LISTEN goalpost_full_${this.name}`, (err) => {

              /* $lab:coverage:off$ */
              if (err) {
                return reject(err);
              }
              /* $lab:coverage:on$ */
              client.query(`LISTEN goalpost_hint_${this.name}`, (err) => {

                /* $lab:coverage:off$ */
                if (err) {
                  return reject(err);
                }
                /* $lab:coverage:on$ */
                return resolve();
              });
            });
          });
        });
      })
      .then(() => {

        this.exists = true;
        return db;
      });
    }

    size(obj) {

      return this._ready()
      .then(() => {

        return db.func(sizeofFunc, [this.name], qrm.one)
        .then((result) => {

          return parseInt(result[sizeofFunc.toLowerCase()], 10);
        });
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

    update(id, obj) {

      return this._ready()
      .then(() => {

        return db.func(updateFunc, [this.name, id, obj], qrm.one);
      })
      .then((results) => {

        const obj2 = results.doc;
        obj2[this.primary] = results.id;
        return obj2;
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

      return this.find(opts);
    }

  }

  Object.assign(Collection.prototype, EventEmitter.prototype);

  return {
    Collection,
    funcs: {
      where: whereFunc,
      find: findFunc
    }
  };
};
