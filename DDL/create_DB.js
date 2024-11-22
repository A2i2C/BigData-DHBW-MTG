db.createUser({
  user: 'rootuser',
  pwd: 'rootpassword',
  roles: [
    {
      role: 'readWrite',
      db: 'mtg_database'
    }
  ]
});

db.createCollection('mtg_cards');

db.mtg_cards.createIndex({ id: 1 }, { unique: true });