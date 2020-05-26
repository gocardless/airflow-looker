# v0.0.1

Initial release with a single operator:

* `LookerUpdateDataGroupByIDOperator`
  * Calls the [`update_datagroup` API](https://docs.looker.com/reference/api-and-integration/api-reference/v3.0/datagroup#update_datagroup).

And the following hook methods:

* `call`
  * Call the Looker API
* `get_look_sql`
  * Gets a SQL query from a Looker look resource and returns the SQL as a string.
