import pyspark.sql.functions as f
from pyspark.sql import SparkSession, WindowSpec, Window, DataFrame, Column

from minsait.ttaa.datio.common.Constants import *
from minsait.ttaa.datio.common.naming.PlayerInput import *
from minsait.ttaa.datio.common.naming.PlayerOutput import *
from minsait.ttaa.datio.utils.Writer import Writer


class Transformer(Writer):
    def __init__(self, spark: SparkSession, flag):
        self.spark: SparkSession = spark
        df: DataFrame = self.read_input()
        df.printSchema()
        df = self.clean_data(df)
        df = self.example_window_function(df)
        df = self.column_selection(df)
        df = self.column_selection_two(df) #exercise 1
        df = self.add_column_player_cat(df) #exercise 2
        df = self.add_column_potential_vs_overall(df) #exercise 3
        df = self.age_validation(df, flag) #exercise 5
        df = self.filter_playercat_potentialvsoverall(df) #exercise 4
        
        # for show 100 records after your transformations and show the DataFrame schema
        df.show(n=100, truncate=False)
        df.printSchema()

        # Uncomment when you want write your final output
        self.write(df)

    def read_input(self) -> DataFrame:
        """
        :return: a DataFrame readed from csv file
        """
        return self.spark.read \
            .option(INFER_SCHEMA, True) \
            .option(HEADER, True) \
            .csv(INPUT_PATH)

    def clean_data(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with filter transformation applied
        column team_position != null && column short_name != null && column overall != null
        """
        df = df.filter(
            (short_name.column().isNotNull()) &
            (short_name.column().isNotNull()) &
            (overall.column().isNotNull()) &
            (team_position.column().isNotNull())
        )
        return df

    def column_selection(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just 5 columns...
        """
        df = df.select(
            short_name.column(),
            overall.column(),
            height_cm.column(),
            team_position.column(),
            catHeightByPosition.column()
        )
        return df

    def example_window_function(self, df: DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have team_position and height_cm columns)
        :return: add to the DataFrame the column "cat_height_by_position"
             by each position value
             cat A for if is in 20 players tallest
             cat B for if is in 50 players tallest
             cat C for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(team_position.column()) \
            .orderBy(height_cm.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank < 10, "A") \
            .when(rank < 50, "B") \
            .otherwise("C")

        df = df.withColumn(catHeightByPosition.name, rule)
        return df
    
    def column_selection_two(self, df:Dataframe) -> DataFrame:
        """
        :param df: is a DataFrame with players information
        :return: a DataFrame with just a 10 columns...
        """
        df = df.select(
            short_name.column(),
            long_name.column(),
            age.column(),
            height_cm.column(),
            weight_kg.column(),
            nationality.column(),
            club_name.column(),
            overall.column(),
            potential.column(),
            team_position.column
        )
        return df
    
    def add_column_player_cat(self, df:DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have nationality, team_position and overall columns)
        :return: add to the DataFrame the column "player_cat"
             by each position value
             cat A for if is one of the best 3 players in his position in his country
             cat B for if is one of the best 5 players in his position in his country
             cat C for if is one of the best 10 players in his position in his country
             cat D for the rest
        """
        w: WindowSpec = Window \
            .partitionBy(nationality.column(), team_position.column()) \
            .orderBy(overall.column().desc())
        rank: Column = f.rank().over(w)

        rule: Column = f.when(rank <= 3, "A") \
            .when(rank <= 5, "B") \
            .when(rank <= 10, "C")
            .otherwise("D")

        df = df.withColumn(playerCat.name, rule)
        return df

    def add_column_potential_vs_overall(self, df:DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have potential and overall columns)
        :return: add to the DataFrame the column "potential_vs_overall"
             column potential divided by overall
        """
        df = df.withColumn(potentialVsOverall.name, potential.column()/overall.column())

    def age_validation(self, df: DataFrame, flag) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have all columns)
        :return: select df by validation
            if parameter flag is 1 perform all steps only for players under 23 years of age
            if paramater flag is 0 for all players
        """
        df = df.filter(age.column() < 23) if flag==1 elif flag==0 df
        return df

    def filter_playercat_potentialvsoverall(self, df:DataFrame) -> DataFrame:
        """
        :param df: is a DataFrame with players information (must have player_cat and potential_vs_overall columns)
        :return: filter by
            if player_cat is A or B
            if player_cat is C and potential_vs_overall is greater than 1.15
            if player_cat is D and potential_vs_overall is greater than 1.25
        """
        df = df.filter(
            playerCat.column().isin(["A","B"]) \
            | ((playerCat.column() == "C") & (potentialVsOverall.column() > 1.15 )) \
            | ((playerCat.column() == "D") & (potentialVsOverall.column() > 1.25 )) 
            )

        return df