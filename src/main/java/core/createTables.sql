DROP TABLE airbuses, routes, companies, countries;

CREATE TABLE companies (
  id   SERIAL PRIMARY KEY,
  name VARCHAR(64)
);

CREATE TABLE countries (
  id   SERIAL PRIMARY KEY,
  name VARCHAR(64)
);

CREATE TABLE airbuses (
  id    SERIAL PRIMARY KEY,
  model VARCHAR(64)
);

CREATE TABLE pilots (
  id         SERIAL PRIMARY KEY,
  firstname  VARCHAR(64),
  lastname   VARCHAR(64),
  company_id INTEGER,
  CONSTRAINT pilots_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies (id)
);

CREATE TABLE routes (
  id           SERIAL PRIMARY KEY,
  from_country INTEGER,
  to_country   INTEGER,
  CONSTRAINT route_from_fk FOREIGN KEY (from_country) REFERENCES countries (id),
  CONSTRAINT route_to_fk FOREIGN KEY (to_country) REFERENCES countries (id)
);

CREATE TABLE companies_airbuses (
  id         SERIAL PRIMARY KEY,
  company_id INTEGER,
  airbus_id  INTEGER,
  CONSTRAINT companies_airbuses_to_companies_fk FOREIGN KEY (company_id) REFERENCES companies (id),
  CONSTRAINT companies_airbuses_to_airbuses_fk FOREIGN KEY (airbus_id) REFERENCES airbuses (id)
);

CREATE TABLE companies_routes_airbuses (
  id         SERIAL PRIMARY KEY,
  route_id   INTEGER,
  com_bus_id INTEGER,
  CONSTRAINT companies_routes_buses_to_routes_fk FOREIGN KEY (route_id) REFERENCES routes (id),
  CONSTRAINT companies_routes_buses_to_companies_buses_fk FOREIGN KEY (com_bus_id) REFERENCES companies_airbuses (id)
);

CREATE TABLE flights (
  id              SERIAL PRIMARY KEY,
  com_rout_bus_id INTEGER,
  first_pilot     INTEGER,
  second_pilot    INTEGER,
  departure       TIMESTAMP,
  arriwe          TIMESTAMP,
  CONSTRAINT flights_to_companies_routes_buses_fk FOREIGN KEY (com_rout_bus_id) REFERENCES companies_routes_airbuses (id),
  CONSTRAINT flights_to_first_pilot_fk FOREIGN KEY (first_pilot) REFERENCES pilots (id),
  CONSTRAINT flights_to_second_pilot_fk FOREIGN KEY (second_pilot) REFERENCES pilots (id)
);