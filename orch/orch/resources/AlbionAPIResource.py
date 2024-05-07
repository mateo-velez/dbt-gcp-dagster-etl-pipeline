from dagster import ConfigurableResource
import pandas as pd
from typing import List
import requests
import json
from datetime import datetime,timedelta


class AlbionAPIResource(ConfigurableResource):
    host: str

    def get_historical_prices(
        self, from_date: str, to_date: str, items: List[str]
    ) -> pd.DataFrame:
        """
        Fetches the historical prices for the specified items within the given date range.

        Args:
            from_date (str): The start date for the range in 'YYYY-MM-DD' format.
            to_date (str): The end date for the range in 'YYYY-MM-DD' format.
            items (List[str]): A list of item names for which to fetch the historical prices.

        Returns:
            pd.DataFrame: A pandas DataFrame containing the historical prices for the specified items.

        Examples:
            >>> resource.get_historical_prices(
                    from_date="2024-01-01", to_date="2024-01-31", items=["T4_BAG", "T4_2H_CLAYMORE"]
                )
                      item_count  avg_price            timestamp         location item_id quality
                0            490       2564  2024-01-06T00:00:00             1213  T4_BAG       1
                1            465       2640  2024-01-06T06:00:00             1213  T4_BAG       1
                2            645       2580  2024-01-06T12:00:00             1213  T4_BAG       1
                3            520       2615  2024-01-06T18:00:00             1213  T4_BAG       1
                4            250       2532  2024-01-07T00:00:00             1213  T4_BAG       1
                ...          ...        ...                  ...              ...     ...     ...
                9388          56       3466  2024-01-30T00:00:00  Thetford Portal  T4_BAG       4
                9389          40       3336  2024-01-30T06:00:00  Thetford Portal  T4_BAG       4
                9390          80       3606  2024-01-30T12:00:00  Thetford Portal  T4_BAG       4
                9391          83       3754  2024-01-30T18:00:00  Thetford Portal  T4_BAG       4
                9392          53       3573  2024-01-31T00:00:00  Thetford Portal  T4_BAG       4
        """
        items_str = ",".join(items)

        from_date_iso = datetime.strptime(from_date, "%Y-%m-%d").isoformat()
        to_date_iso = (datetime.strptime(to_date, "%Y-%m-%d")-timedelta(seconds=1)).isoformat()
        url = f"https://{self.host}/api/v2/stats/history/{items_str}.json?date={from_date_iso}&end_date={to_date_iso}"
        assert len(url) < 4096, "URL too long"
        response = requests.get(url, headers={"Accept-Encoding": "gzip"})
        response.raise_for_status()
        return pd.json_normalize(
            json.loads(response.text), ["data"], ["location", "item_id", "quality"]
        )

    def get_gold_prices(self, from_date: str, to_date: str) -> pd.DataFrame:
        """
        Retrieves gold prices from the specified date range.

        Args:
            from_date (str): The starting date of the range in the format 'YYYY-MM-DD'.
            to_date (str): The ending date of the range in the format 'YYYY-MM-DD'.

        Returns:
            pd.DataFrame: A DataFrame containing the gold prices for the specified date range.

        Examples:
            >>> resource.get_gold_prices("2024-01-01", "2024-01-31")
                     price            timestamp
                0     4794  2024-01-01T00:00:00
                1     4795  2024-01-01T01:00:00
                2     4793  2024-01-01T02:00:00
                3     4791  2024-01-01T03:00:00
                4     4794  2024-01-01T04:00:00
                ..     ...                  ...
                712   4994  2024-01-30T20:00:00
                713   4990  2024-01-30T21:00:00
                714   4989  2024-01-30T22:00:00
                715   4989  2024-01-30T23:00:00
                716   4990  2024-01-31T00:00:00

            """
        
        from_date_iso = datetime.strptime(from_date, "%Y-%m-%d").isoformat()
        to_date_iso = (datetime.strptime(to_date, "%Y-%m-%d")-timedelta(seconds=1)).isoformat()
        url = f"https://{self.host}/api/v2/stats/gold.json?date={from_date_iso}&end_date={to_date_iso}"
        response = requests.get(url, headers={"Accept-Encoding": "gzip"})
        response.raise_for_status()
        return pd.json_normalize(json.loads(response.text))




if __name__ == "__main__":
    albion = AlbionAPIResource(host="www.albion-online-data.com")
    gold_prices = albion.get_gold_prices("2024-01-01", "2024-01-31")
    print(gold_prices)
    historical_prices = albion.get_historical_prices(
        "2024-01-01", "2024-01-31", ["T4_BAG", "T4_2H_CLAYMORE"]
    )
    print(historical_prices)


    