import unittest
from unittest.mock import patch
import main


class Test_run_query(unittest.TestCase):
    @patch("builtins.print")  # Mock the print function to capture output
    def test_run_query(self, mock_print):

        # Call the connect_to_database function
        main.run_query()

        # Assertions based on the expected output
        mock_print.assert_any_call("Query execution completed.")
        mock_print.assert_any_call("Connection closed.")


if __name__ == "__main__":
    unittest.main()
