# airflow-looker

This is a collection of [Airflow](https://airflow.apache.org/) extensions to provide integration with [Looker](https://www.looker.com).

```py
with DAG(dag_id='looker_update_datagroup', schedule_interval='@daily') as dag:
  LookerUpdateDataGroupByIDOperator(
    datagroup_id=123
  )
```

## Installation

Install from PyPI:

```sh
pip install airflow-looker
```

## Usage

There is one operator currently implemented:

* `LookerUpdateDataGroupByIDOperator`
  * Calls the [`update_datagroup` API](https://docs.looker.com/reference/api-and-integration/api-reference/v3.0/datagroup#update_datagroup). Accepts the following arguments:
    * `datagroup_id`
      * The ID of the datagroup to update. Required.
    * `stale_before`
      * Timestamp before which cache entries are considered stale. Defaults to now.

You can also use the hook directly. The two methods that are implemented for use are:

* `call`
  * Call the Looker API. Accepts the following arguments:
    * `method`
      * The method of the call (`GET`, `POST`, etc). Required.
    * `endpoint`
      * The endpoint to be called i.e. `looks/run/1`. Required.
    * `data`
      * Payload to be uploaded or request parameters. Required.
    * `headers`
      * Additional headers to be passed through as a dictionary. Optional.
* `get_look_sql`
  * Gets an SQL query from a Looker look resource and returns the SQL as a string. Accepts the following arguments:
    * `look_id`
      * Unique identifier for a look resource. Required.

### Connection

To use either the operator or the hook you need to pass in a connection ID. This connection needs to have the the host, the login (`client_id`) and the password (`client_secret`) defined.

To create a connection, follow the [Airflow documentation](https://airflow.apache.org/docs/stable/howto/connection/index.html).

## Building Locally

To install from the repository, it's recommended to first create a virtual environment:

```bash
python3 -m venv .venv

source .venv/bin/activate
```

Install using `pip`:

```bash
pip install .
```

## Testing

To run tests locally, first create a virtual environment (see [Building Locally](https://github.com/gocardless/airflow-looker#building-locally) section)

Install dependencies:

```bash
pip install -e .[dev]
```

Run the tests:

```bash
python -m pytest tests/ -sv
```

## Code style

This project uses [flake8](https://flake8.pycqa.org/en/latest/).

To check your code, first create a virtual environment (see [Building Locally](https://github.com/gocardless/airflow-looker#building-locally) section):

```bash
flake8 airflow_looker/ tests/ setup.py
```

## License & Contributing

* This is available as open source under the terms of the [MIT License](http://opensource.org/licenses/MIT).
* Bug reports and pull requests are welcome on GitHub at https://github.com/gocardless/airflow-looker.

GoCardless ♥ open source. If you do too, come [join us](https://gocardless.com/about/jobs).
