I built this using the cx_Oracle module.

It it for getting data from an Oracle database.  It can also delete table's contents and insert data into a table.
When inserting data, there is a filter function that will change adjust the types of each row and cell based on the type
of column it will be handed to.

This is not really intended for wide spread use, but more of an example to show some of my coding experience.

I used this module in many of my projects at work and it sees updates fairly consistently.

This repository does not contain the data_warehouse_creds.json for security reasons.
The Oracle Bridge is currently configured to allow 2 sets of credentials to help with handling the of personal and
company projects.
The format of the json file is as follows:
{
  "credentials": {
    "private": {
      "username": "USERNAME",
      "password": "PASSWORD"
    },
    "public": {
      "username": "USERNAME",
      "password": "PASSWORD"
    }
  },
  "connections": {
    "prod": {
      "host": "HOST.COM",
      "port": 0000,
      "sid": "SID"
    },
    "dev": {
      "host": "HOST.COM",
      "port": 0000,
      "sid": "SID"
    }
  }
}