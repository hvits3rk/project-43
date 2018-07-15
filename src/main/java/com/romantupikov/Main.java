package com.romantupikov;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.*;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("spark://localhost:7077")
                .appName("World Temperature")
                .getOrCreate();

        runCsvDataFrame(spark, args[0]);

        spark.stop();
    }

    private static void runCsvDataFrame(SparkSession spark, String pathToCsv) {

        Path path = Paths
                .get(System.getenv("OUTPUT_PATH") + "/city_country_world_temperature.parquet");

        Dataset<Row> initDataset = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(pathToCsv);

        initDataset.createOrReplaceTempView("init_dataset");

        Column decade = col("year").minus(col("year").mod(10));
        Column century = col("year").minus(col("year").mod(100));

        Dataset<Row> dataset = initDataset
                .select(col("dt").as("date"),
                        col("AverageTemperature").as("temp"),
                        col("City").as("city"),
                        col("Country").as("country"))
                .withColumn("year", year(col("date")))
                .withColumn("decade", decade)
                .withColumn("century", century);

        WindowSpec cityYearWindow = Window.partitionBy("city", "year")
                .orderBy("date")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec cityDecadeWindow = Window.partitionBy("city", "decade")
                .orderBy("year")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec cityCenturyWindow = Window.partitionBy("city", "century")
                .orderBy("year")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec countryYearWindow = Window.partitionBy("country", "year")
                .orderBy("date")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec countryDecadeWindow = Window.partitionBy("country", "decade")
                .orderBy("year")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec countryCenturyWindow = Window.partitionBy("country", "century")
                .orderBy("year")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec worldYearWindow = Window.partitionBy("year")
                .orderBy("date")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec worldDecadeWindow = Window.partitionBy("decade")
                .orderBy("year")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        WindowSpec worldCenturyWindow = Window.partitionBy("century")
                .orderBy("year")
                .rangeBetween(unboundedPreceding(), unboundedFollowing());

        Column avgCityYearTemp = avg("temp").over(cityYearWindow).as("avg_city_year_temp");
        Column minCityYearTemp = min("temp").over(cityYearWindow).as("min_city_year_temp");
        Column maxCityYearTemp = max("temp").over(cityYearWindow).as("max_city_year_temp");

        Column avgCityDecadeTemp = avg("temp").over(cityDecadeWindow).as("avg_city_decade_temp");
        Column minCityDecadeTemp = min("temp").over(cityDecadeWindow).as("min_city_decade_temp");
        Column maxCityDecadeTemp = max("temp").over(cityDecadeWindow).as("max_city_decade_temp");

        Column avgCityCenturyTemp = avg("temp").over(cityCenturyWindow).as("avg_city_century_temp");
        Column minCityCenturyTemp = min("temp").over(cityCenturyWindow).as("min_city_century_temp");
        Column maxCityCenturyTemp = max("temp").over(cityCenturyWindow).as("max_city_century_temp");

        Column avgCountryYearTemp = avg("temp").over(countryYearWindow).as("avg_country_year_temp");
        Column minCountryYearTemp = min("temp").over(countryYearWindow).as("min_country_year_temp");
        Column maxCountryYearTemp = max("temp").over(countryYearWindow).as("max_country_year_temp");

        Column avgCountryDecadeTemp = avg("temp").over(countryDecadeWindow).as("avg_country_decade_temp");
        Column minCountryDecadeTemp = min("temp").over(countryDecadeWindow).as("min_country_decade_temp");
        Column maxCountryDecadeTemp = max("temp").over(countryDecadeWindow).as("max_country_decade_temp");

        Column avgCountryCenturyTemp = avg("temp").over(countryCenturyWindow).as("avg_country_century_temp");
        Column minCountryCenturyTemp = min("temp").over(countryCenturyWindow).as("min_country_century_temp");
        Column maxCountryCenturyTemp = max("temp").over(countryCenturyWindow).as("max_country_century_temp");

        Column avgWorldYearTemp = avg("temp").over(worldYearWindow).as("avg_world_year_temp");
        Column minWorldYearTemp = min("temp").over(worldYearWindow).as("min_world_year_temp");
        Column maxWorldYearTemp = max("temp").over(worldYearWindow).as("max_world_year_temp");

        Column avgWorldDecadeTemp = avg("temp").over(worldDecadeWindow).as("avg_world_decade_temp");
        Column minWorldDecadeTemp = min("temp").over(worldDecadeWindow).as("min_world_decade_temp");
        Column maxWorldDecadeTemp = max("temp").over(worldDecadeWindow).as("max_world_decade_temp");

        Column avgWorldCenturyTemp = avg("temp").over(worldCenturyWindow).as("avg_world_century_temp");
        Column minWorldCenturyTemp = min("temp").over(worldCenturyWindow).as("min_world_century_temp");
        Column maxWorldCenturyTemp = max("temp").over(worldCenturyWindow).as("max_world_century_temp");

        Dataset<Row> result = dataset
                .select(col("year"),
                        col("city"),
                        col("country"),
                        minCityYearTemp,
                        maxCityYearTemp,
                        avgCityYearTemp,
                        minCityDecadeTemp,
                        maxCityDecadeTemp,
                        avgCityDecadeTemp,
                        minCityCenturyTemp,
                        maxCityCenturyTemp,
                        avgCityCenturyTemp,
                        minCountryYearTemp,
                        maxCountryYearTemp,
                        avgCountryYearTemp,
                        minCountryDecadeTemp,
                        maxCountryDecadeTemp,
                        avgCountryDecadeTemp,
                        minCountryCenturyTemp,
                        maxCountryCenturyTemp,
                        avgCountryCenturyTemp,
                        minWorldYearTemp,
                        maxWorldYearTemp,
                        avgWorldYearTemp,
                        minWorldDecadeTemp,
                        maxWorldDecadeTemp,
                        avgWorldDecadeTemp,
                        minWorldCenturyTemp,
                        maxWorldCenturyTemp,
                        avgWorldCenturyTemp)
                .distinct()
                .orderBy("country", "city", "year");

        Map<String, String> options = new HashMap<>();
        options.put("path", path.toString());

        result.write().mode(SaveMode.Overwrite).options(options).save();
    }

}
