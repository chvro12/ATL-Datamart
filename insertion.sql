
INSERT INTO datamart_taxi.dimension_temps (date_heure, heure, jour, mois, annee)
SELECT DISTINCT tpep_pickup_datetime,
       EXTRACT(HOUR FROM tpep_pickup_datetime) AS heure,
       EXTRACT(DAY FROM tpep_pickup_datetime) AS jour,
       EXTRACT(MONTH FROM tpep_pickup_datetime) AS mois,
       EXTRACT(YEAR FROM tpep_pickup_datetime) AS annee
FROM public.nyc_raw;

INSERT INTO datamart_taxi.dimension_lieu (location_id)
SELECT DISTINCT pulocationid
FROM public.nyc_raw;

INSERT INTO datamart_taxi.dimension_lieu (location_id)
SELECT DISTINCT dolocationid
FROM public.nyc_raw
WHERE NOT EXISTS (
    SELECT 1 FROM datamart_taxi.dimension_lieu WHERE location_id = dolocationid
);

INSERT INTO datamart_taxi.dimension_paiement (type_paiement)
SELECT DISTINCT payment_type
FROM public.nyc_raw;

INSERT INTO datamart_taxi.faits_trajets (temps_id, lieu_depart_id, lieu_arrivee_id, paiement_id, distance, montant_total)
SELECT
    dt.temps_id,
    dl.lieu_id AS lieu_depart_id,
    dl2.lieu_id AS lieu_arrivee_id,
    dp.paiement_id,
    nr.trip_distance,
    nr.total_amount
FROM public.nyc_raw nr
JOIN datamart_taxi.dimension_temps dt ON dt.date_heure = nr.tpep_pickup_datetime
JOIN datamart_taxi.dimension_lieu dl ON dl.location_id = nr.pulocationid
JOIN datamart_taxi.dimension_lieu dl2 ON dl2.location_id = nr.dolocationid
JOIN datamart_taxi.dimension_paiement dp ON dp.type_paiement = nr.payment_type;
