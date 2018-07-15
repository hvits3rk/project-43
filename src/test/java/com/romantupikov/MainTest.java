package com.romantupikov;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.junit.jupiter.api.*;

import java.io.Serializable;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.*;
import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MainTest implements Serializable {

    private transient SparkSession spark;
    private Dataset<Row> ds;

    @BeforeAll
    void setUp() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("testing")
                .getOrCreate();

        ds = spark.read().format("csv")
                .option("sep", ",")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("src/test/java/resources/test.csv");
    }

    @Test
    void schemaFieldNames() {
//        date,city,country,temp
        String[] columnNames = ds.schema().fieldNames();

        assertEquals("date", columnNames[0]);
        assertEquals("city", columnNames[1]);
        assertEquals("country", columnNames[2]);
        assertEquals("temp", columnNames[3]);
    }

    @Test
    void dataCount() {
        assertEquals(16L, ds.count());
    }

    @Test
    void dataFromTable() {
        List<Row> data = ds.sort("city", "country").collectAsList();

        assertAll("data",
                () -> assertEquals("1910-11-01 00:00:00.0", data.get(0).getTimestamp(0).toString()),
                () -> assertEquals("city1", data.get(0).getString(1)),
                () -> assertEquals("country1", data.get(0).getString(2)),
                () -> assertEquals(5.0d, data.get(0).getDouble(3))
        );
    }

    @Test
    void addColumnYearTransformTimestampToYearTypeInt() {
        Dataset<Row> transformedDs = ds
                .withColumn("year", year(col("date")))
                .sort("date");

        String[] columnNames = transformedDs.schema().fieldNames();
        List<Row> data = transformedDs.collectAsList();

        assertAll("data",
                () -> assertEquals("year", columnNames[4]),
                () -> assertEquals(1700, data.get(0).getInt(4))
        );
    }

    @Test
    void addColumnsDecadeCentury() {
        Dataset<Row> transformedDs = ds
                .withColumn("year", year(col("date")))
                .sort("date");

        Column decade = col("year").minus(col("year").mod(10));
        Column century = col("year").minus(col("year").mod(100));

        Dataset<Row> result = transformedDs
                .withColumn("decade", decade)
                .withColumn("century", century);

        String[] columnNames = result.schema().fieldNames();
        List<Row> data = result.collectAsList();

        assertAll("data",
                // date, city, country, temp, year, decade, century
                () -> assertEquals(7, columnNames.length),
                () -> assertEquals("year", columnNames[4]),
                () -> assertEquals("decade", columnNames[5]),
                () -> assertEquals("century", columnNames[6]),
                () -> assertEquals(1700, data.get(0).getInt(4)),
                () -> assertEquals(1700, data.get(0).getInt(5)),
                () -> assertEquals(1700, data.get(0).getInt(6)),
                () -> assertEquals(1713, data.get(1).getInt(4)),
                () -> assertEquals(1710, data.get(1).getInt(5)),
                () -> assertEquals(1700, data.get(1).getInt(6)),
                () -> assertEquals(1716, data.get(2).getInt(4)),
                () -> assertEquals(1710, data.get(2).getInt(5)),
                () -> assertEquals(1700, data.get(2).getInt(6)),
                () -> assertEquals(1799, data.get(3).getInt(4)),
                () -> assertEquals(1790, data.get(3).getInt(5)),
                () -> assertEquals(1700, data.get(3).getInt(6))
        );
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class YearWindowTests {
        Dataset<Row> transformedDs;

        @BeforeAll
        void setUp() {
            transformedDs = ds
                    .withColumn("year", year(col("date")));
        }

        @Test
        void cityYearWindowAvgMinMaxTemp() {
            WindowSpec cityYearWindow = Window.partitionBy("city", "year")
                    .orderBy("date")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgCityYearTemp = avg("temp").over(cityYearWindow).as("avg_city_year_temp");
            Column minCityYearTemp = min("temp").over(cityYearWindow).as("min_city_year_temp");
            Column maxCityYearTemp = max("temp").over(cityYearWindow).as("max_city_year_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            avgCityYearTemp,
                            minCityYearTemp,
                            maxCityYearTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(6, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city1", data.get(0).getString(0)),
                    () -> assertEquals("country1", data.get(0).getString(1)),
                    () -> assertEquals(1910, data.get(0).getInt(2)),
                    () -> assertEquals("avg_city_year_temp", columnNames[3]),
                    () -> assertEquals("min_city_year_temp", columnNames[4]),
                    () -> assertEquals("max_city_year_temp", columnNames[5]),
                    () -> assertEquals((5.0 + 10.0) / 2, data.get(0).getDouble(3)),
                    () -> assertEquals(5.0, data.get(0).getDouble(4)),
                    () -> assertEquals(10.0, data.get(0).getDouble(5))
            );
        }

        @Test
        void countryYearWindowAvgMinMaxTemp() {
            WindowSpec countryYearWindow = Window.partitionBy("country", "year")
                    .orderBy("date")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgCountryYearTemp = avg("temp").over(countryYearWindow).as("avg_country_year_temp");
            Column minCountryYearTemp = min("temp").over(countryYearWindow).as("min_country_year_temp");
            Column maxCountryYearTemp = max("temp").over(countryYearWindow).as("max_country_year_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            avgCountryYearTemp,
                            minCountryYearTemp,
                            maxCountryYearTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(6, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city1", data.get(0).getString(0)),
                    () -> assertEquals("country1", data.get(0).getString(1)),
                    () -> assertEquals(1910, data.get(0).getInt(2)),
                    () -> assertEquals("avg_country_year_temp", columnNames[3]),
                    () -> assertEquals("min_country_year_temp", columnNames[4]),
                    () -> assertEquals("max_country_year_temp", columnNames[5]),
                    () -> assertEquals((5.0 + 10.0 + 15.0 + 20.0) / 4, data.get(0).getDouble(3)),
                    () -> assertEquals(5.0, data.get(0).getDouble(4)),
                    () -> assertEquals(20.0, data.get(0).getDouble(5))
            );
        }

        @Test
        void worldYearWindowAvgMinMaxTemp() {
            WindowSpec worldYearWindow = Window.partitionBy("year")
                    .orderBy("date")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgWorldYearTemp = avg("temp").over(worldYearWindow).as("avg_world_year_temp");
            Column minWorldYearTemp = min("temp").over(worldYearWindow).as("min_world_year_temp");
            Column maxWorldYearTemp = max("temp").over(worldYearWindow).as("max_world_year_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            avgWorldYearTemp,
                            minWorldYearTemp,
                            maxWorldYearTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(6, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city1", data.get(0).getString(0)),
                    () -> assertEquals("country1", data.get(0).getString(1)),
                    () -> assertEquals(1910, data.get(0).getInt(2)),
                    () -> assertEquals("avg_world_year_temp", columnNames[3]),
                    () -> assertEquals("min_world_year_temp", columnNames[4]),
                    () -> assertEquals("max_world_year_temp", columnNames[5]),
                    () -> assertEquals((5.0 + 10.0 + 15.0 + 20.0 + 30.0 + 0.0) / 6, data.get(0).getDouble(3)),
                    () -> assertEquals(0.0, data.get(0).getDouble(4)),
                    () -> assertEquals(30.0, data.get(0).getDouble(5))
            );
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class DecadeWindowTests {
        Dataset<Row> transformedDs;

        @BeforeAll
        void setUp() {
            Column decade = col("year").minus(col("year").mod(10));

            transformedDs = ds
                    .withColumn("year", year(col("date")))
                    .withColumn("decade", decade)
                    .sort("date");
        }

        @Test
        void cityDecadeWindowAvgMinMaxTemp() {
            WindowSpec cityDecadeWindow = Window.partitionBy("city", "decade")
                    .orderBy("year")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgCityDecadeTemp = avg("temp").over(cityDecadeWindow).as("avg_city_decade_temp");
            Column minCityDecadeTemp = min("temp").over(cityDecadeWindow).as("min_city_decade_temp");
            Column maxCityDecadeTemp = max("temp").over(cityDecadeWindow).as("max_city_decade_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            col("decade"),
                            avgCityDecadeTemp,
                            minCityDecadeTemp,
                            maxCityDecadeTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(7, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city2", data.get(2).getString(0)),
                    () -> assertEquals("country1", data.get(2).getString(1)),
                    () -> assertEquals(1910, data.get(2).getInt(2)),
                    () -> assertEquals(1910, data.get(2).getInt(3)),
                    () -> assertEquals("avg_city_decade_temp", columnNames[4]),
                    () -> assertEquals("min_city_decade_temp", columnNames[5]),
                    () -> assertEquals("max_city_decade_temp", columnNames[6]),
                    () -> assertEquals((15.0 + 20.0) / 2, data.get(2).getDouble(4)),
                    () -> assertEquals(15.0, data.get(2).getDouble(5)),
                    () -> assertEquals(20.0, data.get(2).getDouble(6))
            );
        }

        @Test
        void countryDecadeWindowAvgMinMaxTemp() {
            WindowSpec countryDecadeWindow = Window.partitionBy("country", "decade")
                    .orderBy("year")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgCountryDecadeTemp = avg("temp").over(countryDecadeWindow).as("avg_country_decade_temp");
            Column minCountryDecadeTemp = min("temp").over(countryDecadeWindow).as("min_country_decade_temp");
            Column maxCountryDecadeTemp = max("temp").over(countryDecadeWindow).as("max_country_decade_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            col("decade"),
                            avgCountryDecadeTemp,
                            minCountryDecadeTemp,
                            maxCountryDecadeTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(7, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city1", data.get(0).getString(0)),
                    () -> assertEquals("country1", data.get(0).getString(1)),
                    () -> assertEquals(1910, data.get(0).getInt(2)),
                    () -> assertEquals(1910, data.get(0).getInt(3)),
                    () -> assertEquals("avg_country_decade_temp", columnNames[4]),
                    () -> assertEquals("min_country_decade_temp", columnNames[5]),
                    () -> assertEquals("max_country_decade_temp", columnNames[6]),
                    () -> assertEquals((5.0 + 10.0 + 15.0 + 20.0) / 4, data.get(0).getDouble(4)),
                    () -> assertEquals(5.0, data.get(0).getDouble(5)),
                    () -> assertEquals(20.0, data.get(0).getDouble(6))
            );
        }

        @Test
        void worldDecadeWindowAvgMinMaxTemp() {
            WindowSpec worldDecadeWindow = Window.partitionBy("decade")
                    .orderBy("year")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgWorldDecadeTemp = avg("temp").over(worldDecadeWindow).as("avg_world_decade_temp");
            Column minWorldDecadeTemp = min("temp").over(worldDecadeWindow).as("min_world_decade_temp");
            Column maxWorldDecadeTemp = max("temp").over(worldDecadeWindow).as("max_world_decade_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            col("decade"),
                            avgWorldDecadeTemp,
                            minWorldDecadeTemp,
                            maxWorldDecadeTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(7, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city3", data.get(4).getString(0)),
                    () -> assertEquals("country1", data.get(4).getString(1)),
                    () -> assertEquals(2011, data.get(4).getInt(2)),
                    () -> assertEquals(2010, data.get(4).getInt(3)),
                    () -> assertEquals("avg_world_decade_temp", columnNames[4]),
                    () -> assertEquals("min_world_decade_temp", columnNames[5]),
                    () -> assertEquals("max_world_decade_temp", columnNames[6]),
                    () -> assertEquals((30.0 + 35.0) / 2, data.get(4).getDouble(4)),
                    () -> assertEquals(30.0, data.get(4).getDouble(5)),
                    () -> assertEquals(35.0, data.get(4).getDouble(6))
            );
        }

    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class CenturyWindowTests {
        Dataset<Row> transformedDs;

        @BeforeAll
        void setUp() {
            Column century = col("year").minus(col("year").mod(100));

            transformedDs = ds
                    .withColumn("year", year(col("date")))
                    .withColumn("century", century)
                    .sort("date");
        }

        @Test
        void cityCenturyWindowAvgMinMaxTemp() {
            WindowSpec cityCenturyWindow = Window.partitionBy("city", "century")
                    .orderBy("year")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgCityCenturyTemp = avg("temp").over(cityCenturyWindow).as("avg_city_century_temp");
            Column minCityCenturyTemp = min("temp").over(cityCenturyWindow).as("min_city_century_temp");
            Column maxCityCenturyTemp = max("temp").over(cityCenturyWindow).as("max_city_century_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            col("century"),
                            avgCityCenturyTemp,
                            minCityCenturyTemp,
                            maxCityCenturyTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(7, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city6", data.get(10).getString(0)),
                    () -> assertEquals("country2", data.get(10).getString(1)),
                    () -> assertEquals(1700, data.get(10).getInt(2)),
                    () -> assertEquals(1700, data.get(10).getInt(3)),
                    () -> assertEquals("avg_city_century_temp", columnNames[4]),
                    () -> assertEquals("min_city_century_temp", columnNames[5]),
                    () -> assertEquals("max_city_century_temp", columnNames[6]),
                    () -> assertEquals((40.0 + 40.0) / 2, data.get(10).getDouble(4)),
                    () -> assertEquals(40.0, data.get(10).getDouble(5)),
                    () -> assertEquals(40.0, data.get(10).getDouble(6))
            );
        }

        @Test
        void countryCenturyWindowAvgMinMaxTemp() {
            WindowSpec countryCenturyWindow = Window.partitionBy("country", "century")
                    .orderBy("year")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgCountryCenturyTemp = avg("temp").over(countryCenturyWindow).as("avg_country_century_temp");
            Column minCountryCenturyTemp = min("temp").over(countryCenturyWindow).as("min_country_century_temp");
            Column maxCountryCenturyTemp = max("temp").over(countryCenturyWindow).as("max_country_century_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            col("century"),
                            avgCountryCenturyTemp,
                            minCountryCenturyTemp,
                            maxCountryCenturyTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(7, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city6", data.get(10).getString(0)),
                    () -> assertEquals("country2", data.get(10).getString(1)),
                    () -> assertEquals(1700, data.get(10).getInt(2)),
                    () -> assertEquals(1700, data.get(10).getInt(3)),
                    () -> assertEquals("avg_country_century_temp", columnNames[4]),
                    () -> assertEquals("min_country_century_temp", columnNames[5]),
                    () -> assertEquals("max_country_century_temp", columnNames[6]),
                    () -> assertEquals((40.0 + 40.0 + 30.0 + 45.0) / 4, data.get(10).getDouble(4)),
                    () -> assertEquals(30.0, data.get(10).getDouble(5)),
                    () -> assertEquals(45.0, data.get(10).getDouble(6))
            );
        }

        @Test
        void worldCenturyWindowAvgMinMaxTemp() {
            WindowSpec worldCenturyWindow = Window.partitionBy("century")
                    .orderBy("year")
                    .rangeBetween(unboundedPreceding(), unboundedFollowing());

            Column avgWorldCenturyTemp = avg("temp").over(worldCenturyWindow).as("avg_world_century_temp");
            Column minWorldCenturyTemp = min("temp").over(worldCenturyWindow).as("min_world_century_temp");
            Column maxWorldCenturyTemp = max("temp").over(worldCenturyWindow).as("max_world_century_temp");

            Dataset<Row> result = transformedDs
                    .select(col("city"),
                            col("country"),
                            col("year"),
                            col("century"),
                            avgWorldCenturyTemp,
                            minWorldCenturyTemp,
                            maxWorldCenturyTemp)
                    .orderBy("country", "city", "year");

            String[] columnNames = result.schema().fieldNames();
            List<Row> data = result.collectAsList();

            assertAll("data",
                    () -> assertEquals(7, columnNames.length),
                    () -> assertEquals(16, data.size()),
                    () -> assertEquals("city1", data.get(0).getString(0)),
                    () -> assertEquals("country1", data.get(0).getString(1)),
                    () -> assertEquals(1910, data.get(0).getInt(2)),
                    () -> assertEquals(1900, data.get(0).getInt(3)),
                    () -> assertEquals("avg_world_century_temp", columnNames[4]),
                    () -> assertEquals("min_world_century_temp", columnNames[5]),
                    () -> assertEquals("max_world_century_temp", columnNames[6]),
                    () -> assertEquals((5.0 + 10.0 + 15.0 + 20.0 + 30.0 + 0.0) / 6, data.get(0).getDouble(4)),
                    () -> assertEquals(0.0, data.get(0).getDouble(5)),
                    () -> assertEquals(30.0, data.get(0).getDouble(6))
            );
        }
    }

    @Nested
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class parquetReadWriteTests {

        Dataset<Row> transformedDs;
        Path path;

        @BeforeAll
        void setUp() {
            path = Paths.get(System.getProperty("user.home") + "/Documents/test.parquet");

            Column century = col("year").minus(col("year").mod(100));
            Column decade = col("year").minus(col("year").mod(10));
            transformedDs = ds
                    .withColumn("year", year(col("date")))
                    .withColumn("decade", decade)
                    .withColumn("century", century)
                    .sort("date");
        }

        @Test
        void parquetReadWrite() {

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

            Dataset<Row> result = transformedDs
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

            String[] savedColumnNames = result.schema().fieldNames();
            List<Row> savedData = result.collectAsList();

            Map<String, String> options = new HashMap<>();
            options.put("path", path.toString());
            result.write().mode(SaveMode.Overwrite).options(options).save();

            Dataset<Row> loadedDs = spark.read()
                    .options(options)
                    .parquet()
                    .orderBy("country", "city", "year");

            String[] loadedColumnNames = loadedDs.schema().fieldNames();
            List<Row> loadedData = loadedDs.collectAsList();

            assertAll("data",
                    () -> assertEquals(savedColumnNames.length, loadedColumnNames.length),
                    () -> assertEquals(savedData.size(), loadedData.size()),
                    () -> assertArrayEquals(savedColumnNames, loadedColumnNames),
                    () -> assertArrayEquals(savedData.toArray(), loadedData.toArray())
            );
        }
    }

    @AfterAll
    void tearDown() {
        spark.stop();
        spark = null;
    }
}