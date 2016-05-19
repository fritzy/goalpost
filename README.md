# Goalpost

Developers love the experience of working with queryable document-stores like CouchDB and RethinkDB.
Postgresql is arguably the most flexible and mature opensource database, which in the past few releases has given us JSON features in addition to it's flexibility and power.

The goal of the project is to give you the same developer experience of a document-store, with the maturity and power of Postgres.

##Example

```js
'use strict';

const Goalpost = require('goalpost')('postgres://fritzy@localhost/goalpost_test');

const myCollection = new Goalpost.Collection({
  name: 'mine',
  primary: 'id'
});

myCollection.put({family: {dogs: 2, kids: 2, wife: 1}, work: {coworkers: 22}})
.then((result) => {
  console.log(result);
  // {id: 3433, family: {dogs: 2, kids: 2, wife: 1}, work: {coworkers: 22}}
  return myCollection.find({where: {dogs: 2, kids: {op: '<', value: 2}, $or: {kids: null}}});
})
.then((results) => {
  // ...
});

myCollection.on('changeFull', (change) => {
  // {collection: 'mine', change: 'put', pk: 3433,
  // full: {id: 3433, family: {dogs: 2, kids: 2, wife: 1}, work: {coworkers: 22}}}
});

myCollection.on('changeHint', (change) => {
  // {collection: 'mine', change: 'put', pk: 3433}
});

myCollection.changesSince(someTimeStamp, {limit: 10})
.then((changes) => {
  // ..
});
```

