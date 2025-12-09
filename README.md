# Trail Condition ETL

The trail condition ETL pipeline is for extracting weather data from a third party service, writing it to internal data stores, and classifying trail conditions based on weather data.

See `DESIGN.md` for design details.

## Getting started

### Running unit tests

Each folder (`schedule-jobs`, `fetch-weather`, `categorize-trail-conditions`) is a stand-alone python package managed by `uv`.
Unit tests can be executed within those folders using `uv run pytest`.

### End-to-end testing

This repository is a microservice within a larger project called `Outpost`.

The repository `https://github.com/brian-burrows/outpost-dev` provides a `docker-compose.yaml` file that provides end-to-end testing of the project. Clone that repository, execute the `setup.sh` script, and run `docker compose up --build -d` to start all relevant services.
