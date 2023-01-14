# luftdaten
analyse der sensordaten luftdaten.at

# Download der luftdaten
Beispiel für einen Tag (das Gleiche nochmal mit *.csv*)

wget -A "*sps30*.csv" -r -np -nc -l1 --no-check-certificate -e robots=off http://archive.sensor.community/2022-12-13/
