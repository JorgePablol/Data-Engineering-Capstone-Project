class z3Interface:
    """Class that defines the methods that must be developed for z3 object."""

    def extract_and_transform_each_provider_and_client(self):
        """Reads the data from the data sources and performs quantitative
        transformations."""
        pass

    def load(self):
        """Gets the processed that into a star schema and loads it into the z3
        database."""
        pass

    def data_quality_checks(self):
        """Tests if the processed data matches the result loaded into the
        database."""
        pass

    @staticmethod
    def drop_tables():
        """Simply drops the tables, this method has to be initialized by the
        user only."""
        pass

    def main(self):
        """Starts the whole ETL process."""
        pass
