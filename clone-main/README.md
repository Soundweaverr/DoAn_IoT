# clone
-b1:Mở mysql và thêm cái này 
-b2:mở terminal chạy npm i để cài đặt thư viên
- b3:mở terminal chạy npm start

//Sql
CREATE DATABASE weather_data;
USE weather_data;

CREATE TABLE weatherData (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station VARCHAR(255),
    air_temperature FLOAT,
    wind_speed_km_per_hour FLOAT,
    humidity INT,
    wave_height_in_meter FLOAT,
    water_temperature FLOAT,
    UV_index INT
);
ALTER TABLE weatherData
ADD timestamp DATETIME

drop table weatherData;

select * from weatherData;
