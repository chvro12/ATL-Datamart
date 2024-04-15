CREATE SCHEMA IF NOT EXISTS datamart_taxi;

CREATE TABLE datamart_taxi.dimension_temps (
    temps_id SERIAL PRIMARY KEY,
    date_heure TIMESTAMP NOT NULL,
    heure INT,
    jour INT,
    mois INT,
    annee INT
);

CREATE TABLE datamart_taxi.dimension_lieu (
    lieu_id SERIAL PRIMARY KEY,
    location_id INT NOT NULL
);

CREATE TABLE datamart_taxi.dimension_paiement (
    paiement_id SERIAL PRIMARY KEY,
    type_paiement INT
);
